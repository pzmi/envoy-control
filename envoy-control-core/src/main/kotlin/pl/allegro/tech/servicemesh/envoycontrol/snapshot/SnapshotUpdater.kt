package pl.allegro.tech.servicemesh.envoycontrol.snapshot

import io.envoyproxy.controlplane.cache.SnapshotCache
import io.envoyproxy.controlplane.cache.v3.Snapshot
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import pl.allegro.tech.servicemesh.envoycontrol.groups.CommunicationMode.ADS
import pl.allegro.tech.servicemesh.envoycontrol.groups.CommunicationMode.XDS
import pl.allegro.tech.servicemesh.envoycontrol.groups.Group
import pl.allegro.tech.servicemesh.envoycontrol.logger
import pl.allegro.tech.servicemesh.envoycontrol.services.MultiClusterState
import pl.allegro.tech.servicemesh.envoycontrol.snapshot.resource.listeners.filters.EnvoyHttpFilters
import pl.allegro.tech.servicemesh.envoycontrol.snapshot.resource.routes.ServiceTagMetadataGenerator
import pl.allegro.tech.servicemesh.envoycontrol.utils.ParallelizableScheduler
import pl.allegro.tech.servicemesh.envoycontrol.utils.doOnNextScheduledOn
import pl.allegro.tech.servicemesh.envoycontrol.utils.measureBuffer
import pl.allegro.tech.servicemesh.envoycontrol.utils.noopTimer
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import java.time.Duration

class SnapshotUpdater(
    private val cache: SnapshotCache<Group, Snapshot>,
    private val properties: SnapshotProperties,
    private val snapshotFactory: EnvoySnapshotFactory,
    private val globalSnapshotScheduler: Scheduler,
    private val groupSnapshotScheduler: ParallelizableScheduler,
    private val onGroupAdded: Flux<out List<Group>>,
    private val meterRegistry: MeterRegistry,
    private val versions: SnapshotsVersions,
    envoyHttpFilters: EnvoyHttpFilters = EnvoyHttpFilters.emptyFilters,
    serviceTagFilter: ServiceTagMetadataGenerator = ServiceTagMetadataGenerator(properties.routing.serviceTags)
) {
    companion object {
        private val logger by logger()
    }

    @Volatile
    private var statesAndClusters = StatesAndClusters.initial

    @Volatile
    private var groups = emptyList<Group>()

    @Volatile
    private var clusterState = MultiClusterState.empty()

    @Volatile
    private var adsSnapshot: GlobalSnapshot? = null

    @Volatile
    private var xdsSnapshot: GlobalSnapshot? = null

    private var globalSnapshot: UpdateResult? = null

    fun getGlobalSnapshot(): UpdateResult? {
        return globalSnapshot
    }

    fun start(states: () -> MultiClusterState): Flux<UpdateResult> {
        return Flux.interval(Duration.ofSeconds(1), globalSnapshotScheduler).map {
            val newState = states()
            if (clusterState != newState) {
                val result = services(newState)
                adsSnapshot = result.adsSnapshot
                xdsSnapshot = result.xdsSnapshot
                updateSnapshotForGroups(result)
            }

            val newGroups = cache.groups().toList()
            val addedGroups = groups - newGroups
            groups = newGroups

            if (addedGroups.isNotEmpty()) {

            }


            result
        }.concatMap { result ->
            if (result.adsSnapshot != null || result.xdsSnapshot != null) {
                    // Stateful operation! This is the meat of this processing.
                } else {
                    Mono.empty()
                }
            }
    }

    internal fun groups(): Flux<UpdateResult> {
        // see GroupChangeWatcher
        return onGroupAdded
            .publishOn(globalSnapshotScheduler)
            .measureBuffer("snapshot-updater-groups-published", meterRegistry)
            .checkpoint("snapshot-updater-groups-published")
            .name("snapshot-updater-groups-published").metrics()
            .map { groups ->
                UpdateResult(action = Action.SERVICES_GROUP_ADDED, groups = groups)
            }
            .onErrorResume { e ->
                meterRegistry.counter("snapshot-updater.groups.updates.errors").increment()
                logger.error("Unable to process new group", e)
                Mono.justOrEmpty(UpdateResult(action = Action.ERROR_PROCESSING_CHANGES))
            }
    }

    internal fun services(states: MultiClusterState): UpdateResult {
        val new =
            StatesAndClusters(states, snapshotFactory.clusterConfigurations(states, statesAndClusters.clusters))
            var lastXdsSnapshot: GlobalSnapshot? = null
            var lastAdsSnapshot: GlobalSnapshot? = null

            if (properties.enabledCommunicationModes.xds) {
                lastXdsSnapshot = snapshotFactory.newSnapshot(new.states, new.clusters, XDS)
            }
            if (properties.enabledCommunicationModes.ads) {
                lastAdsSnapshot = snapshotFactory.newSnapshot(new.states, new.clusters, ADS)
            }

            val updateResult = UpdateResult(
                action = Action.ALL_SERVICES_GROUP_ADDED,
                adsSnapshot = lastAdsSnapshot,
                xdsSnapshot = lastXdsSnapshot
            )
            statesAndClusters = new
            globalSnapshot = updateResult
            return updateResult
    }

    private fun snapshotTimer(serviceName: String) = if (properties.metrics.cacheSetSnapshot) {
        meterRegistry.timer("snapshot-updater.set-snapshot.$serviceName.time")
    } else {
        noopTimer
    }

    private fun updateSnapshotForGroup(group: Group, globalSnapshot: GlobalSnapshot) {
        try {
            val groupSnapshot = snapshotFactory.getSnapshotForGroup(group, globalSnapshot)
            snapshotTimer(group.serviceName).record {
                cache.setSnapshot(group, groupSnapshot)
            }
        } catch (e: Throwable) {
            meterRegistry.counter("snapshot-updater.services.${group.serviceName}.updates.errors").increment()
            logger.error("Unable to create snapshot for group ${group.serviceName}", e)
        }
    }

    private val updateSnapshotForGroupsTimer = meterRegistry.timer("snapshot-updater.update-snapshot-for-groups.time")

    private fun updateSnapshotForGroups(
        result: UpdateResult
    ): Mono<UpdateResult> {
        val sample = Timer.start()
        val groups = result.groups
        versions.retainGroups(cache.groups())
        val results = Flux.fromIterable(groups)
            .doOnNextScheduledOn(groupSnapshotScheduler) { group ->
                if (result.adsSnapshot != null && group.communicationMode == ADS) {
                    updateSnapshotForGroup(group, result.adsSnapshot)
                } else if (result.xdsSnapshot != null && group.communicationMode == XDS) {
                    updateSnapshotForGroup(group, result.xdsSnapshot)
                } else {
                    meterRegistry.counter("snapshot-updater.communication-mode.errors").increment()
                    logger.error("Requested snapshot for ${group.communicationMode.name} mode, but it is not here. " +
                        "Handling Envoy with not supported communication mode should have been rejected before." +
                        " Please report this to EC developers.")
                }
            }
        return results.then(Mono.fromCallable {
            sample.stop(updateSnapshotForGroupsTimer)
            result
        })
    }

    private fun Flux<MultiClusterState>.createClusterConfigurations(): Flux<StatesAndClusters> = this
        .scan(StatesAndClusters.initial) { previous, currentStates ->
            StatesAndClusters(
                states = currentStates,
                clusters = snapshotFactory.clusterConfigurations(currentStates, previous.clusters)
            )
        }
        .filter { it !== StatesAndClusters.initial }

    private data class StatesAndClusters(
        val states: MultiClusterState,
        val clusters: Map<String, ClusterConfiguration>
    ) {
        companion object {
            val initial = StatesAndClusters(MultiClusterState.empty(), emptyMap())
        }
    }
}

enum class Action {
    SERVICES_GROUP_ADDED, ALL_SERVICES_GROUP_ADDED, ERROR_PROCESSING_CHANGES
}

class UpdateResult(
    val action: Action,
    val groups: List<Group> = listOf(),
    val adsSnapshot: GlobalSnapshot? = null,
    val xdsSnapshot: GlobalSnapshot? = null
)
