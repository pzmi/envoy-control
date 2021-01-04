package pl.allegro.tech.servicemesh.envoycontrol.consul.services

import io.envoyproxy.envoy.api.v2.Cluster
import pl.allegro.tech.servicemesh.envoycontrol.services.LocalClusterStateChanges
import pl.allegro.tech.servicemesh.envoycontrol.services.Locality
import pl.allegro.tech.servicemesh.envoycontrol.services.ClusterState
import pl.allegro.tech.servicemesh.envoycontrol.services.MultiClusterState
import pl.allegro.tech.servicemesh.envoycontrol.services.MultiClusterState.Companion.toMultiClusterState
import pl.allegro.tech.servicemesh.envoycontrol.services.ServicesState
import pl.allegro.tech.servicemesh.envoycontrol.services.transformers.ServiceInstancesTransformer
import reactor.core.publisher.Flux
import java.util.concurrent.atomic.AtomicReference

class ConsulLocalClusterStateChanges(
    private val consulChanges: ConsulServiceChanges,
    private val locality: Locality,
    private val cluster: String,
    private val transformers: List<ServiceInstancesTransformer> = emptyList(),
    override val latestServiceState: AtomicReference<ServicesState> = AtomicReference(ServicesState())
) : LocalClusterStateChanges {
    override fun stream(): MultiClusterState {
        val state = consulChanges.getState()
        val servicesState = transformers
            .fold(state.allInstances().asSequence()) { instancesSequence, transformer ->
                transformer.transform(instancesSequence)
            }
            .associateBy { it.serviceName }
            .let(::ServicesState)
        latestServiceState.set(servicesState)
        return ClusterState(servicesState, locality, cluster).toMultiClusterState()
    }

    override fun isInitialStateLoaded(): Boolean = latestServiceState.get() != ServicesState()
}
