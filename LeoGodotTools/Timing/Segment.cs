namespace LeoGodotTools.Timing;

/// <summary>
///     The timing segment that a coroutine is running in or should be run in.
/// </summary>
public enum Segment
{
    /// <summary>
    ///     Sometimes returned as an error state
    /// </summary>
    Invalid = -1,

    /// <summary>
    ///     This is the default timing segment
    /// </summary>
    Process,

    /// <summary>
    ///     This is primarily used for physics calculations
    /// </summary>
    PhysicsProcess,

    /// <summary>
    ///     This is run immediately after update
    /// </summary>
    DeferredProcess
}
