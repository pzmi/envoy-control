package pl.allegro.tech.servicemesh.envoycontrol.snapshot

import pl.allegro.tech.servicemesh.envoycontrol.groups.Group


interface ServiceTagFilter {
    fun filterTagsForRouting(tags: Set<String>): Set<String>
    fun getRoutingTagsForGroup(group: Group): List<String>
}

class DefaultServiceTagFilter : ServiceTagFilter {
    override fun filterTagsForRouting(tags: Set<String>): Set<String> = tags
    override fun getRoutingTagsForGroup(group: Group): List<String> = emptyList()
}