using Godot;

namespace LeoGodotTools.Timing;

public static class CoroutineExtensions
{
    /// <summary>
    ///     Run a new coroutine in the Process segment.
    /// </summary>
    /// <param name="coroutine">The new coroutine's handle.</param>
    /// <returns>The coroutine's handle, which can be used for Wait and Kill operations.</returns>
    public static CoroutineHandle RunCoroutine(this IEnumerator<double> coroutine) => Timing.RunCoroutine(coroutine);

    /// <summary>
    ///     Run a new coroutine in the Process segment.
    /// </summary>
    /// <param name="coroutine">The new coroutine's handle.</param>
    /// <param name="tag">An optional tag to attach to the coroutine which can later be used to identify this coroutine.</param>
    /// <returns>The coroutine's handle, which can be used for Wait and Kill operations.</returns>
    public static CoroutineHandle RunCoroutine(this IEnumerator<double> coroutine, string tag) =>
        Timing.RunCoroutine(coroutine, tag);

    /// <summary>
    ///     Run a new coroutine.
    /// </summary>
    /// <param name="coroutine">The new coroutine's handle.</param>
    /// <param name="segment">The segment that the coroutine should run in.</param>
    /// <returns>The coroutine's handle, which can be used for Wait and Kill operations.</returns>
    public static CoroutineHandle RunCoroutine(this IEnumerator<double> coroutine, Segment segment) =>
        Timing.RunCoroutine(coroutine, segment);

    /// <summary>
    ///     Run a new coroutine.
    /// </summary>
    /// <param name="coroutine">The new coroutine's handle.</param>
    /// <param name="segment">The segment that the coroutine should run in.</param>
    /// <param name="tag">An optional tag to attach to the coroutine which can later be used to identify this coroutine.</param>
    /// <returns>The coroutine's handle, which can be used for Wait and Kill operations.</returns>
    public static CoroutineHandle RunCoroutine(this IEnumerator<double> coroutine, Segment segment, string tag) =>
        Timing.RunCoroutine(coroutine, segment, tag);

    /// <summary>
    ///     Cancels this coroutine when the supplied game object is destroyed or made inactive.
    /// </summary>
    /// <param name="coroutine">The coroutine handle to act upon.</param>
    /// <param name="node">The Node to test.</param>
    /// <returns>The modified coroutine handle.</returns>
    public static IEnumerator<double> CancelWith(this IEnumerator<double> coroutine, Node node)
    {
        while (Timing.MainThread != Thread.CurrentThread || (IsNodeAlive(node) && coroutine.MoveNext()))
        {
            yield return coroutine.Current;
        }
    }

    /// <summary>
    ///     Cancels this coroutine when the supplied game objects are destroyed or made inactive.
    /// </summary>
    /// <param name="coroutine">The coroutine handle to act upon.</param>
    /// <param name="node1">The first Node to test.</param>
    /// <param name="node2">The second Node to test</param>
    /// <returns>The modified coroutine handle.</returns>
    public static IEnumerator<double> CancelWith(this IEnumerator<double> coroutine, Node node1, Node node2)
    {
        while (Timing.MainThread != Thread.CurrentThread ||
               (IsNodeAlive(node1) && IsNodeAlive(node2) && coroutine.MoveNext()))
        {
            yield return coroutine.Current;
        }
    }

    /// <summary>
    ///     Cancels this coroutine when the supplied game objects are destroyed or made inactive.
    /// </summary>
    /// <param name="coroutine">The coroutine handle to act upon.</param>
    /// <param name="node1">The first Node to test.</param>
    /// <param name="node2">The second Node to test</param>
    /// <param name="node3">The third Node to test.</param>
    /// <returns>The modified coroutine handle.</returns>
    public static IEnumerator<double> CancelWith(this IEnumerator<double> coroutine,
                                                 Node node1, Node node2, Node node3)
    {
        while (Timing.MainThread != Thread.CurrentThread ||
               (IsNodeAlive(node1) && IsNodeAlive(node2) && IsNodeAlive(node3) && coroutine.MoveNext()))
        {
            yield return coroutine.Current;
        }
    }

    /// <summary>
    ///     Checks whether a node exists, has not been deleted, and is in a tree
    /// </summary>
    /// <returns></returns>
    private static bool IsNodeAlive(Node? node) => node != null && !node.IsQueuedForDeletion() && node.IsInsideTree();
}
