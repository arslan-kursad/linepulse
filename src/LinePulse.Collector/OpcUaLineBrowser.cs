using Opc.Ua;
using Opc.Ua.Client;

namespace LinePulse.Collector;

/// <summary>A line found by browsing, with the node ids of the tags the collector consumes.</summary>
internal sealed record DiscoveredLine(string Site, string Line, IReadOnlyDictionary<string, NodeId> Tags)
{
    public string Key => $"{Site}/{Line}";
}

/// <summary>
/// Finds every line under <c>Objects/&lt;root&gt;/&lt;site&gt;/&lt;line&gt;</c> and
/// the node ids of its tags, by browsing.
/// </summary>
/// <remarks>
/// Numeric node ids are not hardcoded anywhere on purpose. They are assigned
/// by the server, and a firmware update, a reload of the configuration or one
/// added tag can renumber them; a gateway pinned to numeric ids then reads the
/// wrong tag and reports it confidently. Browse names are part of the server's
/// published interface and survive that, which is also why this runs again on
/// every reconnect instead of being cached.
/// </remarks>
internal static class OpcUaLineBrowser
{
    public static async Task<IReadOnlyList<DiscoveredLine>> DiscoverAsync(
        ISession session,
        string rootName,
        ILogger log,
        CancellationToken ct)
    {
        var browser = new Browser(session, new BrowserOptions
        {
            BrowseDirection = BrowseDirection.Forward,
            ReferenceTypeId = ReferenceTypeIds.HierarchicalReferences,
            IncludeSubtypes = true,
            NodeClassMask = 0,
            ResultMask = (uint)BrowseResultMask.All,
        });

        var result = new List<DiscoveredLine>();

        var root = (await ChildrenAsync(browser, ObjectIds.ObjectsFolder, ct))
            .FirstOrDefault(r => string.Equals(r.BrowseName.Name, rootName, StringComparison.Ordinal));
        if (root is null)
        {
            log.LogWarning("OPC-UA server has no object named {Root} under Objects", rootName);
            return result;
        }

        foreach (var siteRef in await ObjectsUnderAsync(browser, root, session, ct))
        {
            foreach (var lineRef in await ObjectsUnderAsync(browser, siteRef, session, ct))
            {
                var site = siteRef.BrowseName.Name ?? "";
                var line = lineRef.BrowseName.Name ?? "";
                var tags = new Dictionary<string, NodeId>(StringComparer.Ordinal);

                if (ToNodeId(lineRef, session) is not { } lineId)
                {
                    continue;
                }

                foreach (var tagRef in await ChildrenAsync(browser, lineId, ct))
                {
                    if (tagRef.NodeClass != NodeClass.Variable
                        || tagRef.BrowseName.Name is not { } name
                        || ToNodeId(tagRef, session) is not { } tagId)
                    {
                        continue;
                    }

                    if (MachineTags.Subscribed.Contains(name, StringComparer.Ordinal))
                    {
                        tags[name] = tagId;
                    }
                    else
                    {
                        // Worth a line at debug level: a tag the server offers
                        // and the contract has no home for is the usual first
                        // sign that the two have drifted apart.
                        log.LogDebug("{Site}/{Line}: tag {Tag} not consumed", site, line, name);
                    }
                }

                if (tags.Count == 0)
                {
                    continue;
                }

                var discovered = new DiscoveredLine(site, line, tags);
                result.Add(discovered);
                log.LogInformation("discovered {Key} with {Count} tags", discovered.Key, tags.Count);
            }
        }

        return result;
    }

    private static async Task<IEnumerable<ReferenceDescription>> ObjectsUnderAsync(
        Browser browser,
        ReferenceDescription parent,
        ISession session,
        CancellationToken ct)
    {
        if (ToNodeId(parent, session) is not { } parentId)
        {
            return [];
        }

        return (await ChildrenAsync(browser, parentId, ct))
            .Where(r => r.NodeClass == NodeClass.Object && !string.IsNullOrEmpty(r.BrowseName.Name));
    }

    /// <summary>
    /// Copies the browse result out of its span-backed container so it can be
    /// iterated across awaits. A node with no references comes back as a null
    /// array, which means the same as an empty one here.
    /// </summary>
    private static async Task<ReferenceDescription[]> ChildrenAsync(
        Browser browser,
        NodeId parent,
        CancellationToken ct) =>
        (await browser.BrowseAsync(parent, ct)).ToArray() ?? [];

    /// <summary>
    /// Resolves a browse result to a local node id, or <c>null</c> when it points
    /// at a namespace or server this session does not know. Such a node cannot
    /// be read through this session, so it is skipped rather than guessed at.
    /// </summary>
    private static NodeId? ToNodeId(ReferenceDescription reference, ISession session) =>
        ExpandedNodeId.ToNodeId(reference.NodeId, session.NamespaceUris);
}
