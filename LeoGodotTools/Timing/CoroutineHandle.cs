namespace LeoGodotTools.Timing;

/// <summary>
///     A handle for a MEC coroutine.
/// </summary>
public readonly struct CoroutineHandle : IEquatable<CoroutineHandle>
{
    private const byte ReservedSpace = 0x0F;
    private static readonly int[] nextIndex = { ReservedSpace + 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    private readonly int _id;

    public byte Key => (byte) (_id & ReservedSpace);

    public CoroutineHandle(byte ind)
    {
        if (ind > ReservedSpace)
        {
            ind -= ReservedSpace;
        }

        _id = nextIndex[ind] + ind;
        nextIndex[ind] += ReservedSpace + 1;
    }

    public bool Equals(CoroutineHandle other) => _id == other._id;

    public override bool Equals(object? other)
    {
        if (other is CoroutineHandle handle)
        {
            return Equals(handle);
        }

        return false;
    }

    public static bool operator ==(CoroutineHandle a, CoroutineHandle b) => a._id == b._id;

    public static bool operator !=(CoroutineHandle a, CoroutineHandle b) => a._id != b._id;

    public override int GetHashCode() => _id;

    /// <summary>
    ///     Is true if this handle may have been a valid handle at some point. (i.e. is not an uninitialized handle, error
    ///     handle, or a key to a coroutine lock)
    /// </summary>
    public bool IsValid => Key != 0;
}
