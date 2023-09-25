using System.Reflection;
using Godot;

namespace LeoGodotTools.Timing;

// TODO further refactor this class
public partial class Timing : Node
{
    /// <summary>
    ///     You can use "yield return Timing.WaitForOneFrame;" inside a coroutine function to go to the next frame.
    /// </summary>
    public const double WaitForOneFrame = double.NegativeInfinity;

    private const ushort FramesUntilMaintenance = 64;
    private const int ProcessArrayChunkSize = 64;
    private const int InitialBufferSizeLarge = 256;
    private const int InitialBufferSizeMedium = 64;
    private const int InitialBufferSizeSmall = 8;

    /// <summary>
    ///     Used for advanced coroutine control.
    /// </summary>
    public static Func<IEnumerator<double>, CoroutineHandle, IEnumerator<double>?> ReplacementFunction;

    private static object _tmpRef;

    private static readonly Timing[] ActiveInstances = new Timing[16];
    private static Timing? _instance;
    private readonly HashSet<CoroutineHandle> _allWaiting = new HashSet<CoroutineHandle>();

    private readonly Dictionary<CoroutineHandle, ProcessIndex> _handleToIndex
        = new Dictionary<CoroutineHandle, ProcessIndex>();

    private readonly Dictionary<ProcessIndex, CoroutineHandle> _indexToHandle
        = new Dictionary<ProcessIndex, CoroutineHandle>();

    private readonly Dictionary<CoroutineHandle, string> _processTags = new Dictionary<CoroutineHandle, string>();

    private readonly Dictionary<string, HashSet<CoroutineHandle>> _taggedProcesses
        = new Dictionary<string, HashSet<CoroutineHandle>>();

    private readonly Dictionary<CoroutineHandle, HashSet<CoroutineHandle>> _waitingTriggers
        = new Dictionary<CoroutineHandle, HashSet<CoroutineHandle>>();

    private ulong _currentDeferredProcessFrame;

    private ulong _currentProcessFrame;
    private ushort _expansions = 1;
    private ushort _framesSinceProcess;
    private byte _instanceID;
    private int _lastDeferredProcessProcessSlot;
    private double _lastDeferredProcessTime;
    private int _lastPhysicsProcessProcessSlot;
    private double _lastPhysicsProcessTime;
    private int _lastProcessProcessSlot;
    private double _lastProcessTime;
    private int _nextDeferredProcessProcessSlot;
    private int _nextPhysicsProcessProcessSlot;
    private int _nextProcessProcessSlot;
    private double _physicsProcessTime;

    /// <summary>
    ///     The number of coroutines that are being run in the DeferredProcess segment.
    ///     Note, this is updated every <variable>FramesUntilMaintenance</variable> frames.
    /// </summary>
    [Export]
    public int DeferredProcessCoroutines;

    private bool[] DeferredProcessHeld = new bool[InitialBufferSizeSmall];
    private bool[] DeferredProcessPaused = new bool[InitialBufferSizeSmall];
    private IEnumerator<double>?[] DeferredProcessProcesses = new IEnumerator<double>?[InitialBufferSizeSmall];

    /// <summary>
    ///     The amount of time in fractional seconds that elapsed between this frame and the last frame.
    /// </summary>
    [NonSerialized]
    public double deltaTime;

    /// <summary>
    ///     The time in seconds that the current segment has been running.
    /// </summary>
    [NonSerialized]
    public double localTime;

    /// <summary>
    ///     The number of coroutines that are being run in the PhysicsProcess segment.
    ///     Note, this is updated every <variable>FramesUntilMaintenance</variable> frames.
    /// </summary>
    [Export]
    public int PhysicsProcessCoroutines;

    private bool[] PhysicsProcessHeld = new bool[InitialBufferSizeMedium];
    private bool[] PhysicsProcessPaused = new bool[InitialBufferSizeMedium];
    private IEnumerator<double>?[] PhysicsProcessProcesses = new IEnumerator<double>?[InitialBufferSizeMedium];

    /// <summary>
    ///     The number of coroutines that are being run in the Process segment.
    ///     Note, this is updated every <variable>FramesUntilMaintenance</variable> frames.
    /// </summary>
    [Export]
    public int ProcessCoroutines;

    private bool[] ProcessHeld = new bool[InitialBufferSizeLarge];

    private bool[] ProcessPaused = new bool[InitialBufferSizeLarge];

    private IEnumerator<double>?[] ProcessProcesses = new IEnumerator<double>?[InitialBufferSizeLarge];

    public Timing()
    {
        InitializeInstanceID();
    }

    /// <summary>
    ///     The amount of time in fractional seconds that elapsed between this frame and the last frame.
    /// </summary>
    public static double DeltaTime
    {
        get { return Instance.deltaTime; }
    }

    /// <summary>
    ///     The main thread that (almost) everything in godot runs in.
    /// </summary>
    public static Thread MainThread { get; private set; }

    /// <summary>
    ///     The handle of the current coroutine that is running.
    /// </summary>
    public CoroutineHandle currentCoroutine { get; private set; }

    public static Timing Instance
    {
        get
        {
            if (_instance == null)
            {
                // Check if we were loaded via Autoload
                _instance = ((SceneTree) Engine.GetMainLoop()).Root.GetNodeOrNull<Timing>(typeof(Timing).Name);
                if (_instance == null)
                {
                    // Instantiate to root at runtime
                    _instance = new Timing();
                    _instance.Name = typeof(Timing).Name;
                    _instance.CallDeferred(nameof(InitGlobalInstance));
                }
            }

            return _instance;
        }
    }

    private void InitGlobalInstance()
    {
        ((SceneTree) Engine.GetMainLoop()).Root.AddChild(this);
    }

    public override void _Ready()
    {
        // We process before other nodes by default
        ProcessPriority = -1;

        // Godot 4.1 only, 4.0 does not implement this.
        // Use reflection to try and set it for compatibility.
        //ProcessPhysicsPriority = -1;
        try
        {
            GetType().GetProperty("ProcessPhysicsPriority",
                                  BindingFlags.Instance |
                                  BindingFlags.Public).SetValue(this, -1);
        }
        catch (NullReferenceException) { }

        if (MainThread == null)
        {
            MainThread = Thread.CurrentThread;
        }
    }

    public override void _ExitTree()
    {
        if (_instanceID < ActiveInstances.Length)
        {
            ActiveInstances[_instanceID] = null;
        }
    }

    private void InitializeInstanceID()
    {
        if (ActiveInstances[_instanceID] == null)
        {
            if (_instanceID == 0x00)
            {
                _instanceID++;
            }

            for (; _instanceID <= 0x10; _instanceID++)
            {
                if (_instanceID == 0x10)
                {
                    QueueFree();

                    throw new
                        OverflowException("You are only allowed 15 different contexts for MEC to run inside at one time.");
                }

                if (ActiveInstances[_instanceID] == null)
                {
                    ActiveInstances[_instanceID] = this;

                    break;
                }
            }
        }
    }

    public override void _Process(double delta)
    {
        if (_nextProcessProcessSlot > 0)
        {
            var coindex = new ProcessIndex { Seg = Segment.Process };
            if (UpdateTimeValues(coindex.Seg))
            {
                _lastProcessProcessSlot = _nextProcessProcessSlot;
            }

            for (coindex.I = 0; coindex.I < _lastProcessProcessSlot; coindex.I++)
            {
                try
                {
                    if (!ProcessPaused[coindex.I] && !ProcessHeld[coindex.I] && ProcessProcesses[coindex.I] != null &&
                        !(localTime < ProcessProcesses[coindex.I].Current))
                    {
                        currentCoroutine = _indexToHandle[coindex];

                        if (!ProcessProcesses[coindex.I].MoveNext())
                        {
                            if (_indexToHandle.ContainsKey(coindex))
                            {
                                KillCoroutinesOnInstance(_indexToHandle[coindex]);
                            }
                        }
                        else if (ProcessProcesses[coindex.I] != null &&
                                 double.IsNaN(ProcessProcesses[coindex.I].Current))
                        {
                            if (ReplacementFunction != null)
                            {
                                ProcessProcesses[coindex.I]
                                    = ReplacementFunction(ProcessProcesses[coindex.I], _indexToHandle[coindex]);
                                ReplacementFunction = null;
                            }

                            coindex.I--;
                        }
                    }
                }
                catch (Exception ex)
                {
                    GD.PrintErr(ex);
                }
            }
        }

        currentCoroutine = default(CoroutineHandle);

        if (++_framesSinceProcess > FramesUntilMaintenance)
        {
            _framesSinceProcess = 0;

            RemoveUnused();
        }

        CallDeferred(nameof(_DeferredProcess));
    }

    public override void _PhysicsProcess(double delta)
    {
        if (_nextPhysicsProcessProcessSlot > 0)
        {
            var coindex = new ProcessIndex { Seg = Segment.PhysicsProcess };
            if (UpdateTimeValues(coindex.Seg))
            {
                _lastPhysicsProcessProcessSlot = _nextPhysicsProcessProcessSlot;
            }

            for (coindex.I = 0; coindex.I < _lastPhysicsProcessProcessSlot; coindex.I++)
            {
                try
                {
                    if (!PhysicsProcessPaused[coindex.I] && !PhysicsProcessHeld[coindex.I] &&
                        PhysicsProcessProcesses[coindex.I] != null &&
                        !(localTime < PhysicsProcessProcesses[coindex.I].Current))
                    {
                        currentCoroutine = _indexToHandle[coindex];

                        if (!PhysicsProcessProcesses[coindex.I].MoveNext())
                        {
                            if (_indexToHandle.ContainsKey(coindex))
                            {
                                KillCoroutinesOnInstance(_indexToHandle[coindex]);
                            }
                        }
                        else if (PhysicsProcessProcesses[coindex.I] != null &&
                                 double.IsNaN(PhysicsProcessProcesses[coindex.I].Current))
                        {
                            if (ReplacementFunction != null)
                            {
                                PhysicsProcessProcesses[coindex.I]
                                    = ReplacementFunction(PhysicsProcessProcesses[coindex.I], _indexToHandle[coindex]);
                                ReplacementFunction = null;
                            }

                            coindex.I--;
                        }
                    }
                }
                catch (Exception ex)
                {
                    GD.PrintErr(ex);
                }
            }

            currentCoroutine = default;
        }
    }

    private void _DeferredProcess()
    {
        if (_nextDeferredProcessProcessSlot > 0)
        {
            var coindex = new ProcessIndex { Seg = Segment.DeferredProcess };
            if (UpdateTimeValues(coindex.Seg))
            {
                _lastDeferredProcessProcessSlot = _nextDeferredProcessProcessSlot;
            }

            for (coindex.I = 0; coindex.I < _lastDeferredProcessProcessSlot; coindex.I++)
            {
                try
                {
                    if (!DeferredProcessPaused[coindex.I] && !DeferredProcessHeld[coindex.I] &&
                        DeferredProcessProcesses[coindex.I] != null &&
                        !(localTime < DeferredProcessProcesses[coindex.I].Current))
                    {
                        currentCoroutine = _indexToHandle[coindex];

                        if (!DeferredProcessProcesses[coindex.I].MoveNext())
                        {
                            if (_indexToHandle.ContainsKey(coindex))
                            {
                                KillCoroutinesOnInstance(_indexToHandle[coindex]);
                            }
                        }
                        else if (DeferredProcessProcesses[coindex.I] != null &&
                                 double.IsNaN(DeferredProcessProcesses[coindex.I].Current))
                        {
                            if (ReplacementFunction != null)
                            {
                                DeferredProcessProcesses[coindex.I]
                                    = ReplacementFunction(DeferredProcessProcesses[coindex.I], _indexToHandle[coindex]);
                                ReplacementFunction = null;
                            }

                            coindex.I--;
                        }
                    }
                }
                catch (Exception ex)
                {
                    GD.PrintErr(ex);
                }
            }

            currentCoroutine = default(CoroutineHandle);
        }
    }

    private void RemoveUnused()
    {
        var waitTrigsEnum = _waitingTriggers.GetEnumerator();
        while (waitTrigsEnum.MoveNext())
        {
            if (waitTrigsEnum.Current.Value.Count == 0)
            {
                _waitingTriggers.Remove(waitTrigsEnum.Current.Key);
                waitTrigsEnum = _waitingTriggers.GetEnumerator();

                continue;
            }

            if (_handleToIndex.ContainsKey(waitTrigsEnum.Current.Key) &&
                CoindexIsNull(_handleToIndex[waitTrigsEnum.Current.Key]))
            {
                CloseWaitingProcess(waitTrigsEnum.Current.Key);
                waitTrigsEnum = _waitingTriggers.GetEnumerator();
            }
        }

        ProcessIndex outer, inner;
        outer.Seg = inner.Seg = Segment.Process;

        for (outer.I = inner.I = 0; outer.I < _nextProcessProcessSlot; outer.I++)
        {
            if (ProcessProcesses[outer.I] != null)
            {
                if (outer.I != inner.I)
                {
                    ProcessProcesses[inner.I] = ProcessProcesses[outer.I];
                    ProcessPaused[inner.I] = ProcessPaused[outer.I];
                    ProcessHeld[inner.I] = ProcessHeld[outer.I];

                    if (_indexToHandle.ContainsKey(inner))
                    {
                        RemoveTag(_indexToHandle[inner]);
                        _handleToIndex.Remove(_indexToHandle[inner]);
                        _indexToHandle.Remove(inner);
                    }

                    _handleToIndex[_indexToHandle[outer]] = inner;
                    _indexToHandle.Add(inner, _indexToHandle[outer]);
                    _indexToHandle.Remove(outer);
                }

                inner.I++;
            }
        }

        for (outer.I = inner.I; outer.I < _nextProcessProcessSlot; outer.I++)
        {
            ProcessProcesses[outer.I] = null;
            ProcessPaused[outer.I] = false;
            ProcessHeld[outer.I] = false;

            if (_indexToHandle.ContainsKey(outer))
            {
                RemoveTag(_indexToHandle[outer]);

                _handleToIndex.Remove(_indexToHandle[outer]);
                _indexToHandle.Remove(outer);
            }
        }

        _lastProcessProcessSlot -= _nextProcessProcessSlot - inner.I;
        ProcessCoroutines = _nextProcessProcessSlot = inner.I;

        outer.Seg = inner.Seg = Segment.PhysicsProcess;
        for (outer.I = inner.I = 0; outer.I < _nextPhysicsProcessProcessSlot; outer.I++)
        {
            if (PhysicsProcessProcesses[outer.I] != null)
            {
                if (outer.I != inner.I)
                {
                    PhysicsProcessProcesses[inner.I] = PhysicsProcessProcesses[outer.I];
                    PhysicsProcessPaused[inner.I] = PhysicsProcessPaused[outer.I];
                    PhysicsProcessHeld[inner.I] = PhysicsProcessHeld[outer.I];

                    if (_indexToHandle.ContainsKey(inner))
                    {
                        RemoveTag(_indexToHandle[inner]);
                        _handleToIndex.Remove(_indexToHandle[inner]);
                        _indexToHandle.Remove(inner);
                    }

                    _handleToIndex[_indexToHandle[outer]] = inner;
                    _indexToHandle.Add(inner, _indexToHandle[outer]);
                    _indexToHandle.Remove(outer);
                }

                inner.I++;
            }
        }

        for (outer.I = inner.I; outer.I < _nextPhysicsProcessProcessSlot; outer.I++)
        {
            PhysicsProcessProcesses[outer.I] = null;
            PhysicsProcessPaused[outer.I] = false;
            PhysicsProcessHeld[outer.I] = false;

            if (_indexToHandle.ContainsKey(outer))
            {
                RemoveTag(_indexToHandle[outer]);

                _handleToIndex.Remove(_indexToHandle[outer]);
                _indexToHandle.Remove(outer);
            }
        }

        _lastPhysicsProcessProcessSlot -= _nextPhysicsProcessProcessSlot - inner.I;
        PhysicsProcessCoroutines = _nextPhysicsProcessProcessSlot = inner.I;

        outer.Seg = inner.Seg = Segment.DeferredProcess;
        for (outer.I = inner.I = 0; outer.I < _nextDeferredProcessProcessSlot; outer.I++)
        {
            if (DeferredProcessProcesses[outer.I] != null)
            {
                if (outer.I != inner.I)
                {
                    DeferredProcessProcesses[inner.I] = DeferredProcessProcesses[outer.I];
                    DeferredProcessPaused[inner.I] = DeferredProcessPaused[outer.I];
                    DeferredProcessHeld[inner.I] = DeferredProcessHeld[outer.I];

                    if (_indexToHandle.ContainsKey(inner))
                    {
                        RemoveTag(_indexToHandle[inner]);
                        _handleToIndex.Remove(_indexToHandle[inner]);
                        _indexToHandle.Remove(inner);
                    }

                    _handleToIndex[_indexToHandle[outer]] = inner;
                    _indexToHandle.Add(inner, _indexToHandle[outer]);
                    _indexToHandle.Remove(outer);
                }

                inner.I++;
            }
        }

        for (outer.I = inner.I; outer.I < _nextDeferredProcessProcessSlot; outer.I++)
        {
            DeferredProcessProcesses[outer.I] = null;
            DeferredProcessPaused[outer.I] = false;
            DeferredProcessHeld[outer.I] = false;

            if (_indexToHandle.ContainsKey(outer))
            {
                RemoveTag(_indexToHandle[outer]);

                _handleToIndex.Remove(_indexToHandle[outer]);
                _indexToHandle.Remove(outer);
            }
        }

        _lastDeferredProcessProcessSlot -= _nextDeferredProcessProcessSlot - inner.I;
        DeferredProcessCoroutines = _nextDeferredProcessProcessSlot = inner.I;
    }

    /// <summary>
    ///     Run a new coroutine in the Process segment.
    /// </summary>
    /// <param name="coroutine">The new coroutine's handle.</param>
    /// <returns>The coroutine's handle, which can be used for Wait and Kill operations.</returns>
    public static CoroutineHandle RunCoroutine(IEnumerator<double> coroutine)
    {
        return coroutine == null
            ? new CoroutineHandle()
            : Instance.RunCoroutineInternal(coroutine, Segment.Process, null, new CoroutineHandle(Instance._instanceID),
                                            true);
    }

    /// <summary>
    ///     Run a new coroutine in the Process segment.
    /// </summary>
    /// <param name="coroutine">The new coroutine's handle.</param>
    /// <param name="tag">An optional tag to attach to the coroutine which can later be used for Kill operations.</param>
    /// <returns>The coroutine's handle, which can be used for Wait and Kill operations.</returns>
    public static CoroutineHandle RunCoroutine(IEnumerator<double> coroutine, string tag)
    {
        return coroutine == null
            ? new CoroutineHandle()
            : Instance.RunCoroutineInternal(coroutine, Segment.Process, tag, new CoroutineHandle(Instance._instanceID),
                                            true);
    }

    /// <summary>
    ///     Run a new coroutine.
    /// </summary>
    /// <param name="coroutine">The new coroutine's handle.</param>
    /// <param name="segment">The segment that the coroutine should run in.</param>
    /// <returns>The coroutine's handle, which can be used for Wait and Kill operations.</returns>
    public static CoroutineHandle RunCoroutine(IEnumerator<double> coroutine, Segment segment)
    {
        return coroutine == null
            ? new CoroutineHandle()
            : Instance.RunCoroutineInternal(coroutine, segment, null, new CoroutineHandle(Instance._instanceID), true);
    }

    /// <summary>
    ///     Run a new coroutine.
    /// </summary>
    /// <param name="coroutine">The new coroutine's handle.</param>
    /// <param name="segment">The segment that the coroutine should run in.</param>
    /// <param name="tag">An optional tag to attach to the coroutine which can later be used for Kill operations.</param>
    /// <returns>The coroutine's handle, which can be used for Wait and Kill operations.</returns>
    public static CoroutineHandle RunCoroutine(IEnumerator<double> coroutine, Segment segment, string tag)
    {
        return coroutine == null
            ? new CoroutineHandle()
            : Instance.RunCoroutineInternal(coroutine, segment, tag, new CoroutineHandle(Instance._instanceID), true);
    }

    /// <summary>
    ///     Run a new coroutine on this Timing instance in the Process segment.
    /// </summary>
    /// <param name="coroutine">The new coroutine's handle.</param>
    /// <returns>The coroutine's handle, which can be used for Wait and Kill operations.</returns>
    public CoroutineHandle RunCoroutineOnInstance(IEnumerator<double> coroutine)
    {
        return coroutine == null
            ? new CoroutineHandle()
            : RunCoroutineInternal(coroutine, Segment.Process, null, new CoroutineHandle(_instanceID), true);
    }

    /// <summary>
    ///     Run a new coroutine on this Timing instance in the Process segment.
    /// </summary>
    /// <param name="coroutine">The new coroutine's handle.</param>
    /// <param name="tag">An optional tag to attach to the coroutine which can later be used for Kill operations.</param>
    /// <returns>The coroutine's handle, which can be used for Wait and Kill operations.</returns>
    public CoroutineHandle RunCoroutineOnInstance(IEnumerator<double> coroutine, string tag)
    {
        return coroutine == null
            ? new CoroutineHandle()
            : RunCoroutineInternal(coroutine, Segment.Process, tag, new CoroutineHandle(_instanceID), true);
    }

    /// <summary>
    ///     Run a new coroutine on this Timing instance.
    /// </summary>
    /// <param name="coroutine">The new coroutine's handle.</param>
    /// <param name="segment">The segment that the coroutine should run in.</param>
    /// <returns>The coroutine's handle, which can be used for Wait and Kill operations.</returns>
    public CoroutineHandle RunCoroutineOnInstance(IEnumerator<double> coroutine, Segment segment)
    {
        return coroutine == null
            ? new CoroutineHandle()
            : RunCoroutineInternal(coroutine, segment, null, new CoroutineHandle(_instanceID), true);
    }

    /// <summary>
    ///     Run a new coroutine on this Timing instance.
    /// </summary>
    /// <param name="coroutine">The new coroutine's handle.</param>
    /// <param name="segment">The segment that the coroutine should run in.</param>
    /// <param name="tag">An optional tag to attach to the coroutine which can later be used for Kill operations.</param>
    /// <returns>The coroutine's handle, which can be used for Wait and Kill operations.</returns>
    public CoroutineHandle RunCoroutineOnInstance(IEnumerator<double> coroutine, Segment segment, string tag)
    {
        return coroutine == null
            ? new CoroutineHandle()
            : RunCoroutineInternal(coroutine, segment, tag, new CoroutineHandle(_instanceID), true);
    }

    private CoroutineHandle RunCoroutineInternal(IEnumerator<double> coroutine, Segment segment, string tag,
                                                 CoroutineHandle handle, bool prewarm)
    {
        var slot = new ProcessIndex { Seg = segment };

        if (_handleToIndex.ContainsKey(handle))
        {
            _indexToHandle.Remove(_handleToIndex[handle]);
            _handleToIndex.Remove(handle);
        }

        var currentLocalTime = localTime;
        var currentDeltaTime = deltaTime;
        var cachedHandle = currentCoroutine;
        currentCoroutine = handle;

        switch (segment)
        {
            case Segment.Process:

                if (_nextProcessProcessSlot >= ProcessProcesses.Length)
                {
                    var oldProcArray = ProcessProcesses;
                    var oldPausedArray = ProcessPaused;
                    var oldHeldArray = ProcessHeld;

                    ProcessProcesses
                        = new IEnumerator<double>[ProcessProcesses.Length + ProcessArrayChunkSize * _expansions++];
                    ProcessPaused = new bool[ProcessProcesses.Length];
                    ProcessHeld = new bool[ProcessProcesses.Length];

                    for (var i = 0; i < oldProcArray.Length; i++)
                    {
                        ProcessProcesses[i] = oldProcArray[i];
                        ProcessPaused[i] = oldPausedArray[i];
                        ProcessHeld[i] = oldHeldArray[i];
                    }
                }

                if (UpdateTimeValues(slot.Seg))
                {
                    _lastProcessProcessSlot = _nextProcessProcessSlot;
                }

                slot.I = _nextProcessProcessSlot++;
                ProcessProcesses[slot.I] = coroutine;

                if (null != tag)
                {
                    AddTag(tag, handle);
                }

                _indexToHandle.Add(slot, handle);
                _handleToIndex.Add(handle, slot);

                while (prewarm)
                {
                    if (!ProcessProcesses[slot.I].MoveNext())
                    {
                        if (_indexToHandle.ContainsKey(slot))
                        {
                            KillCoroutinesOnInstance(_indexToHandle[slot]);
                        }

                        prewarm = false;
                    }
                    else if (ProcessProcesses[slot.I] != null && double.IsNaN(ProcessProcesses[slot.I].Current))
                    {
                        if (ReplacementFunction != null)
                        {
                            ProcessProcesses[slot.I]
                                = ReplacementFunction(ProcessProcesses[slot.I], _indexToHandle[slot]);
                            ReplacementFunction = null;
                        }

                        prewarm = !ProcessPaused[slot.I] && !ProcessHeld[slot.I];
                    }
                    else
                    {
                        prewarm = false;
                    }
                }

                break;

            case Segment.PhysicsProcess:

                if (_nextPhysicsProcessProcessSlot >= PhysicsProcessProcesses.Length)
                {
                    var oldProcArray = PhysicsProcessProcesses;
                    var oldPausedArray = PhysicsProcessPaused;
                    var oldHeldArray = PhysicsProcessHeld;

                    PhysicsProcessProcesses
                        = new IEnumerator<double>[PhysicsProcessProcesses.Length +
                                                  ProcessArrayChunkSize * _expansions++];
                    PhysicsProcessPaused = new bool[PhysicsProcessProcesses.Length];
                    PhysicsProcessHeld = new bool[PhysicsProcessProcesses.Length];

                    for (var i = 0; i < oldProcArray.Length; i++)
                    {
                        PhysicsProcessProcesses[i] = oldProcArray[i];
                        PhysicsProcessPaused[i] = oldPausedArray[i];
                        PhysicsProcessHeld[i] = oldHeldArray[i];
                    }
                }

                if (UpdateTimeValues(slot.Seg))
                {
                    _lastPhysicsProcessProcessSlot = _nextPhysicsProcessProcessSlot;
                }

                slot.I = _nextPhysicsProcessProcessSlot++;
                PhysicsProcessProcesses[slot.I] = coroutine;

                if (null != tag)
                {
                    AddTag(tag, handle);
                }

                _indexToHandle.Add(slot, handle);
                _handleToIndex.Add(handle, slot);

                while (prewarm)
                {
                    if (!PhysicsProcessProcesses[slot.I].MoveNext())
                    {
                        if (_indexToHandle.ContainsKey(slot))
                        {
                            KillCoroutinesOnInstance(_indexToHandle[slot]);
                        }

                        prewarm = false;
                    }
                    else if (PhysicsProcessProcesses[slot.I] != null &&
                             double.IsNaN(PhysicsProcessProcesses[slot.I].Current))
                    {
                        if (ReplacementFunction != null)
                        {
                            PhysicsProcessProcesses[slot.I]
                                = ReplacementFunction(PhysicsProcessProcesses[slot.I], _indexToHandle[slot]);
                            ReplacementFunction = null;
                        }

                        prewarm = !PhysicsProcessPaused[slot.I] && !PhysicsProcessHeld[slot.I];
                    }
                    else
                    {
                        prewarm = false;
                    }
                }

                break;

            case Segment.DeferredProcess:

                if (_nextDeferredProcessProcessSlot >= DeferredProcessProcesses.Length)
                {
                    var oldProcArray = DeferredProcessProcesses;
                    var oldPausedArray = DeferredProcessPaused;
                    var oldHeldArray = DeferredProcessHeld;

                    DeferredProcessProcesses
                        = new IEnumerator<double>[DeferredProcessProcesses.Length +
                                                  ProcessArrayChunkSize * _expansions++];
                    DeferredProcessPaused = new bool[DeferredProcessProcesses.Length];
                    DeferredProcessHeld = new bool[DeferredProcessProcesses.Length];

                    for (var i = 0; i < oldProcArray.Length; i++)
                    {
                        DeferredProcessProcesses[i] = oldProcArray[i];
                        DeferredProcessPaused[i] = oldPausedArray[i];
                        DeferredProcessHeld[i] = oldHeldArray[i];
                    }
                }

                if (UpdateTimeValues(slot.Seg))
                {
                    _lastDeferredProcessProcessSlot = _nextDeferredProcessProcessSlot;
                }

                slot.I = _nextDeferredProcessProcessSlot++;
                DeferredProcessProcesses[slot.I] = coroutine;

                if (tag != null)
                {
                    AddTag(tag, handle);
                }

                _indexToHandle.Add(slot, handle);
                _handleToIndex.Add(handle, slot);

                while (prewarm)
                {
                    if (!DeferredProcessProcesses[slot.I].MoveNext())
                    {
                        if (_indexToHandle.ContainsKey(slot))
                        {
                            KillCoroutinesOnInstance(_indexToHandle[slot]);
                        }

                        prewarm = false;
                    }
                    else if (DeferredProcessProcesses[slot.I] != null &&
                             double.IsNaN(DeferredProcessProcesses[slot.I].Current))
                    {
                        if (ReplacementFunction != null)
                        {
                            DeferredProcessProcesses[slot.I]
                                = ReplacementFunction(DeferredProcessProcesses[slot.I], _indexToHandle[slot]);
                            ReplacementFunction = null;
                        }

                        prewarm = !DeferredProcessPaused[slot.I] && !DeferredProcessHeld[slot.I];
                    }
                    else
                    {
                        prewarm = false;
                    }
                }

                break;

            default:
                handle = new CoroutineHandle();

                break;
        }

        localTime = currentLocalTime;
        deltaTime = currentDeltaTime;
        currentCoroutine = cachedHandle;

        return handle;
    }

    /// <summary>
    ///     This will kill all coroutines running on the main MEC instance and reset the context.
    ///     NOTE: If you call this function from within a running coroutine then you MUST end the current
    ///     coroutine. If the running coroutine has more work to do you may run a new "part 2" coroutine
    ///     function to complete the task before ending the current one.
    /// </summary>
    /// <returns>The number of coroutines that were killed.</returns>
    public static int KillCoroutines()
    {
        return _instance == null ? 0 : _instance.KillCoroutinesOnInstance();
    }

    /// <summary>
    ///     This will kill all coroutines running on the current MEC instance and reset the context.
    ///     NOTE: If you call this function from within a running coroutine then you MUST end the current
    ///     coroutine. If the running coroutine has more work to do you may run a new "part 2" coroutine
    ///     function to complete the task before ending the current one.
    /// </summary>
    /// <returns>The number of coroutines that were killed.</returns>
    public int KillCoroutinesOnInstance()
    {
        var retVal = _nextProcessProcessSlot + _nextDeferredProcessProcessSlot + _nextPhysicsProcessProcessSlot;

        ProcessProcesses = new IEnumerator<double>[InitialBufferSizeLarge];
        ProcessPaused = new bool[InitialBufferSizeLarge];
        ProcessHeld = new bool[InitialBufferSizeLarge];
        ProcessCoroutines = 0;
        _nextProcessProcessSlot = 0;

        DeferredProcessProcesses = new IEnumerator<double>[InitialBufferSizeSmall];
        DeferredProcessPaused = new bool[InitialBufferSizeSmall];
        DeferredProcessHeld = new bool[InitialBufferSizeSmall];
        DeferredProcessCoroutines = 0;
        _nextDeferredProcessProcessSlot = 0;

        PhysicsProcessProcesses = new IEnumerator<double>[InitialBufferSizeMedium];
        PhysicsProcessPaused = new bool[InitialBufferSizeMedium];
        PhysicsProcessHeld = new bool[InitialBufferSizeMedium];
        PhysicsProcessCoroutines = 0;
        _nextPhysicsProcessProcessSlot = 0;

        _processTags.Clear();
        _taggedProcesses.Clear();
        _handleToIndex.Clear();
        _indexToHandle.Clear();
        _waitingTriggers.Clear();
        _expansions = (ushort) (_expansions / 2 + 1);

        return retVal;
    }

    /// <summary>
    ///     Kills the instances of the coroutine handle if it exists.
    /// </summary>
    /// <param name="handle">The handle of the coroutine to kill.</param>
    /// <returns>The number of coroutines that were found and killed (0 or 1).</returns>
    public static int KillCoroutines(CoroutineHandle handle)
    {
        return ActiveInstances[handle.Key] != null ? GetInstance(handle.Key).KillCoroutinesOnInstance(handle) : 0;
    }

    /// <summary>
    ///     Kills the instance of the coroutine handle on this Timing instance if it exists.
    /// </summary>
    /// <param name="handle">The handle of the coroutine to kill.</param>
    /// <returns>The number of coroutines that were found and killed (0 or 1).</returns>
    public int KillCoroutinesOnInstance(CoroutineHandle handle)
    {
        var foundOne = false;

        if (_handleToIndex.ContainsKey(handle))
        {
            if (_waitingTriggers.ContainsKey(handle))
            {
                CloseWaitingProcess(handle);
            }

            foundOne = CoindexExtract(_handleToIndex[handle]) != null;
            RemoveTag(handle);
        }

        return foundOne ? 1 : 0;
    }

    /// <summary>
    ///     Kills all coroutines that have the given tag.
    /// </summary>
    /// <param name="tag">All coroutines with this tag will be killed.</param>
    /// <returns>The number of coroutines that were found and killed.</returns>
    public static int KillCoroutines(string tag)
    {
        return _instance == null ? 0 : _instance.KillCoroutinesOnInstance(tag);
    }

    /// <summary>
    ///     Kills all coroutines that have the given tag.
    /// </summary>
    /// <param name="tag">All coroutines with this tag will be killed.</param>
    /// <returns>The number of coroutines that were found and killed.</returns>
    public int KillCoroutinesOnInstance(string tag)
    {
        if (tag == null)
        {
            return 0;
        }

        var numberFound = 0;

        while (_taggedProcesses.ContainsKey(tag))
        {
            var matchEnum = _taggedProcesses[tag].GetEnumerator();
            matchEnum.MoveNext();

            if (Nullify(_handleToIndex[matchEnum.Current]))
            {
                if (_waitingTriggers.ContainsKey(matchEnum.Current))
                {
                    CloseWaitingProcess(matchEnum.Current);
                }

                numberFound++;
            }

            RemoveTag(matchEnum.Current);

            if (_handleToIndex.ContainsKey(matchEnum.Current))
            {
                _indexToHandle.Remove(_handleToIndex[matchEnum.Current]);
                _handleToIndex.Remove(matchEnum.Current);
            }
        }

        return numberFound;
    }

    /// <summary>
    ///     This will pause all coroutines running on the current MEC instance until ResumeCoroutines is called.
    /// </summary>
    /// <returns>The number of coroutines that were paused.</returns>
    public static int PauseCoroutines()
    {
        return _instance == null ? 0 : _instance.PauseCoroutinesOnInstance();
    }

    /// <summary>
    ///     This will pause all coroutines running on this MEC instance until ResumeCoroutinesOnInstance is called.
    /// </summary>
    /// <returns>The number of coroutines that were paused.</returns>
    public int PauseCoroutinesOnInstance()
    {
        var count = 0;
        int i;
        for (i = 0; i < _nextProcessProcessSlot; i++)
        {
            if (!ProcessPaused[i] && ProcessProcesses[i] != null)
            {
                count++;
                ProcessPaused[i] = true;

                if (ProcessProcesses[i].Current > GetSegmentTime(Segment.Process))
                {
                    ProcessProcesses[i] = _InjectDelay(ProcessProcesses[i],
                                                       ProcessProcesses[i].Current - GetSegmentTime(Segment.Process));
                }
            }
        }

        for (i = 0; i < _nextDeferredProcessProcessSlot; i++)
        {
            if (!DeferredProcessPaused[i] && DeferredProcessProcesses[i] != null)
            {
                count++;
                DeferredProcessPaused[i] = true;

                if (DeferredProcessProcesses[i].Current > GetSegmentTime(Segment.DeferredProcess))
                {
                    DeferredProcessProcesses[i] = _InjectDelay(DeferredProcessProcesses[i],
                                                               DeferredProcessProcesses[i].Current -
                                                               GetSegmentTime(Segment.DeferredProcess));
                }
            }
        }

        for (i = 0; i < _nextPhysicsProcessProcessSlot; i++)
        {
            if (!PhysicsProcessPaused[i] && PhysicsProcessProcesses[i] != null)
            {
                count++;
                PhysicsProcessPaused[i] = true;

                if (PhysicsProcessProcesses[i].Current > GetSegmentTime(Segment.PhysicsProcess))
                {
                    PhysicsProcessProcesses[i] = _InjectDelay(PhysicsProcessProcesses[i],
                                                              PhysicsProcessProcesses[i].Current -
                                                              GetSegmentTime(Segment.PhysicsProcess));
                }
            }
        }

        return count;
    }

    /// <summary>
    ///     This will pause any matching coroutines until ResumeCoroutines is called.
    /// </summary>
    /// <param name="handle">The handle of the coroutine to pause.</param>
    /// <returns>The number of coroutines that were paused (0 or 1).</returns>
    public static int PauseCoroutines(CoroutineHandle handle)
    {
        return ActiveInstances[handle.Key] != null ? GetInstance(handle.Key).PauseCoroutinesOnInstance(handle) : 0;
    }

    /// <summary>
    ///     This will pause any matching coroutines running on this MEC instance until ResumeCoroutinesOnInstance is called.
    /// </summary>
    /// <param name="handle">The handle of the coroutine to pause.</param>
    /// <returns>The number of coroutines that were paused (0 or 1).</returns>
    public int PauseCoroutinesOnInstance(CoroutineHandle handle)
    {
        return _handleToIndex.ContainsKey(handle) && !CoindexIsNull(_handleToIndex[handle]) &&
               !SetPause(_handleToIndex[handle], true)
            ? 1
            : 0;
    }

    /// <summary>
    ///     This will pause any matching coroutines running on the current MEC instance until ResumeCoroutines is called.
    /// </summary>
    /// <param name="tag">Any coroutines with a matching tag will be paused.</param>
    /// <returns>The number of coroutines that were paused.</returns>
    public static int PauseCoroutines(string tag)
    {
        return _instance == null ? 0 : _instance.PauseCoroutinesOnInstance(tag);
    }

    /// <summary>
    ///     This will pause any matching coroutines running on this MEC instance until ResumeCoroutinesOnInstance is called.
    /// </summary>
    /// <param name="tag">Any coroutines with a matching tag will be paused.</param>
    /// <returns>The number of coroutines that were paused.</returns>
    public int PauseCoroutinesOnInstance(string tag)
    {
        if (tag == null || !_taggedProcesses.ContainsKey(tag))
        {
            return 0;
        }

        var count = 0;
        var matchesEnum = _taggedProcesses[tag].GetEnumerator();

        while (matchesEnum.MoveNext())
        {
            if (!CoindexIsNull(_handleToIndex[matchesEnum.Current]) &&
                !SetPause(_handleToIndex[matchesEnum.Current], true))
            {
                count++;
            }
        }

        return count;
    }

    /// <summary>
    ///     This resumes all coroutines on the current MEC instance if they are currently paused, otherwise it has
    ///     no effect.
    /// </summary>
    /// <returns>The number of coroutines that were resumed.</returns>
    public static int ResumeCoroutines()
    {
        return _instance == null ? 0 : _instance.ResumeCoroutinesOnInstance();
    }

    /// <summary>
    ///     This resumes all coroutines on this MEC instance if they are currently paused, otherwise it has no effect.
    /// </summary>
    /// <returns>The number of coroutines that were resumed.</returns>
    public int ResumeCoroutinesOnInstance()
    {
        var count = 0;
        ProcessIndex coindex;
        for (coindex.I = 0, coindex.Seg = Segment.Process; coindex.I < _nextProcessProcessSlot; coindex.I++)
        {
            if (ProcessPaused[coindex.I] && ProcessProcesses[coindex.I] != null)
            {
                ProcessPaused[coindex.I] = false;
                count++;
            }
        }

        for (coindex.I = 0, coindex.Seg = Segment.DeferredProcess; coindex.I < _nextDeferredProcessProcessSlot;
             coindex.I++)
        {
            if (DeferredProcessPaused[coindex.I] && DeferredProcessProcesses[coindex.I] != null)
            {
                DeferredProcessPaused[coindex.I] = false;
                count++;
            }
        }

        for (coindex.I = 0, coindex.Seg = Segment.PhysicsProcess; coindex.I < _nextPhysicsProcessProcessSlot;
             coindex.I++)
        {
            if (PhysicsProcessPaused[coindex.I] && PhysicsProcessProcesses[coindex.I] != null)
            {
                PhysicsProcessPaused[coindex.I] = false;
                count++;
            }
        }

        return count;
    }

    /// <summary>
    ///     This will resume any matching coroutines.
    /// </summary>
    /// <param name="handle">The handle of the coroutine to resume.</param>
    /// <returns>The number of coroutines that were resumed (0 or 1).</returns>
    public static int ResumeCoroutines(CoroutineHandle handle)
    {
        return ActiveInstances[handle.Key] != null ? GetInstance(handle.Key).ResumeCoroutinesOnInstance(handle) : 0;
    }

    /// <summary>
    ///     This will resume any matching coroutines running on this MEC instance.
    /// </summary>
    /// <param name="handle">The handle of the coroutine to resume.</param>
    /// <returns>The number of coroutines that were resumed (0 or 1).</returns>
    public int ResumeCoroutinesOnInstance(CoroutineHandle handle)
    {
        return _handleToIndex.ContainsKey(handle) &&
               !CoindexIsNull(_handleToIndex[handle]) && SetPause(_handleToIndex[handle], false)
            ? 1
            : 0;
    }

    /// <summary>
    ///     This resumes any matching coroutines on the current MEC instance if they are currently paused, otherwise it has
    ///     no effect.
    /// </summary>
    /// <param name="tag">Any coroutines previously paused with a matching tag will be resumend.</param>
    /// <returns>The number of coroutines that were resumed.</returns>
    public static int ResumeCoroutines(string? tag) => _instance?.ResumeCoroutinesOnInstance(tag) ?? 0;

    /// <summary>
    ///     This resumes any matching coroutines on this MEC instance if they are currently paused, otherwise it has no effect.
    /// </summary>
    /// <param name="tag">Any coroutines previously paused with a matching tag will be resumend.</param>
    /// <returns>The number of coroutines that were resumed.</returns>
    public int ResumeCoroutinesOnInstance(string? tag)
    {
        if (tag == null || !_taggedProcesses.ContainsKey(tag))
        {
            return 0;
        }

        var count = 0;

        using var indexesEnum = _taggedProcesses[tag].GetEnumerator();
        while (indexesEnum.MoveNext())
        {
            if (!CoindexIsNull(_handleToIndex[indexesEnum.Current]) &&
                SetPause(_handleToIndex[indexesEnum.Current], false))
            {
                count++;
            }
        }

        return count;
    }

    private bool UpdateTimeValues(Segment segment)
    {
        switch (segment)
        {
            case Segment.Process:
                if (_currentProcessFrame != Engine.GetProcessFrames())
                {
                    deltaTime = GetProcessDeltaTime();
                    _lastProcessTime += deltaTime;
                    localTime = _lastProcessTime;
                    _currentProcessFrame = Engine.GetProcessFrames();

                    return true;
                }

                deltaTime = GetProcessDeltaTime();
                localTime = _lastProcessTime;

                return false;
            case Segment.DeferredProcess:
                if (_currentDeferredProcessFrame != Engine.GetProcessFrames())
                {
                    deltaTime = GetProcessDeltaTime();
                    _lastDeferredProcessTime += deltaTime;
                    localTime = _lastDeferredProcessTime;
                    _currentDeferredProcessFrame = Engine.GetProcessFrames();

                    return true;
                }

                deltaTime = GetProcessDeltaTime();
                localTime = _lastDeferredProcessTime;

                return false;
            case Segment.PhysicsProcess:
                deltaTime = GetPhysicsProcessDeltaTime();
                _physicsProcessTime += deltaTime;
                localTime = _physicsProcessTime;

                if (_lastPhysicsProcessTime + 0.0001f < _physicsProcessTime)
                {
                    _lastPhysicsProcessTime = _physicsProcessTime;

                    return true;
                }

                return false;
        }

        return true;
    }

    private double GetSegmentTime(Segment segment)
    {
        switch (segment)
        {
            case Segment.Process:
                if (_currentProcessFrame == Engine.GetProcessFrames())
                {
                    return _lastProcessTime;
                }

                return _lastProcessTime + GetProcessDeltaTime();
            case Segment.DeferredProcess:
                if (_currentProcessFrame == Engine.GetProcessFrames())
                {
                    return _lastDeferredProcessTime;
                }

                return _lastDeferredProcessTime + GetProcessDeltaTime();
            case Segment.PhysicsProcess:
                return _physicsProcessTime;
            default:
                return 0f;
        }
    }

    /// <summary>
    ///     Retrieves the MEC manager that corresponds to the supplied instance id.
    /// </summary>
    /// <param name="id">The instance ID.</param>
    /// <returns>The manager, or null if not found.</returns>
    public static Timing? GetInstance(byte id) => id >= 0x10 ? null : ActiveInstances[id];

    private void AddTag(string tag, CoroutineHandle coindex)
    {
        _processTags.Add(coindex, tag);

        if (_taggedProcesses.TryGetValue(tag, out var process))
        {
            process.Add(coindex);
        }
        else
        {
            _taggedProcesses.Add(tag, new HashSet<CoroutineHandle> { coindex });
        }
    }

    private void RemoveTag(CoroutineHandle coindex)
    {
        if (_processTags.ContainsKey(coindex))
        {
            if (_taggedProcesses[_processTags[coindex]].Count > 1)
            {
                _taggedProcesses[_processTags[coindex]].Remove(coindex);
            }
            else
            {
                _taggedProcesses.Remove(_processTags[coindex]);
            }

            _processTags.Remove(coindex);
        }
    }

    /// <returns>Whether it was already null.</returns>
    private bool Nullify(ProcessIndex coindex)
    {
        bool retVal;

        switch (coindex.Seg)
        {
            case Segment.Process:
                retVal = ProcessProcesses[coindex.I] != null;
                ProcessProcesses[coindex.I] = null;

                return retVal;
            case Segment.PhysicsProcess:
                retVal = PhysicsProcessProcesses[coindex.I] != null;
                PhysicsProcessProcesses[coindex.I] = null;

                return retVal;
            case Segment.DeferredProcess:
                retVal = DeferredProcessProcesses[coindex.I] != null;
                DeferredProcessProcesses[coindex.I] = null;

                return retVal;
            default:
                return false;
        }
    }

    private IEnumerator<double>? CoindexExtract(ProcessIndex coindex)
    {
        IEnumerator<double> retVal;

        switch (coindex.Seg)
        {
            case Segment.Process:
                retVal = ProcessProcesses[coindex.I];
                ProcessProcesses[coindex.I] = null;

                return retVal;
            case Segment.PhysicsProcess:
                retVal = PhysicsProcessProcesses[coindex.I];
                PhysicsProcessProcesses[coindex.I] = null;

                return retVal;
            case Segment.DeferredProcess:
                retVal = DeferredProcessProcesses[coindex.I];
                DeferredProcessProcesses[coindex.I] = null;

                return retVal;
            default:
                return null;
        }
    }

    private IEnumerator<double>? CoindexPeek(ProcessIndex coindex)
    {
        switch (coindex.Seg)
        {
            case Segment.Process:
                return ProcessProcesses[coindex.I];
            case Segment.PhysicsProcess:
                return PhysicsProcessProcesses[coindex.I];
            case Segment.DeferredProcess:
                return DeferredProcessProcesses[coindex.I];
            default:
                return null;
        }
    }

    private bool CoindexIsNull(ProcessIndex coindex)
    {
        switch (coindex.Seg)
        {
            case Segment.Process:
                return ProcessProcesses[coindex.I] == null;
            case Segment.PhysicsProcess:
                return PhysicsProcessProcesses[coindex.I] == null;
            case Segment.DeferredProcess:
                return DeferredProcessProcesses[coindex.I] == null;
            default:
                return true;
        }
    }

    private bool SetPause(ProcessIndex coindex, bool newPausedState)
    {
        if (CoindexPeek(coindex) == null)
        {
            return false;
        }

        bool isPaused;

        switch (coindex.Seg)
        {
            case Segment.Process:
                isPaused = ProcessPaused[coindex.I];
                ProcessPaused[coindex.I] = newPausedState;

                if (newPausedState && ProcessProcesses[coindex.I].Current > GetSegmentTime(coindex.Seg))
                {
                    ProcessProcesses[coindex.I] = _InjectDelay(ProcessProcesses[coindex.I],
                                                               ProcessProcesses[coindex.I].Current -
                                                               GetSegmentTime(coindex.Seg));
                }

                return isPaused;
            case Segment.PhysicsProcess:
                isPaused = PhysicsProcessPaused[coindex.I];
                PhysicsProcessPaused[coindex.I] = newPausedState;

                if (newPausedState && PhysicsProcessProcesses[coindex.I].Current > GetSegmentTime(coindex.Seg))
                {
                    PhysicsProcessProcesses[coindex.I] = _InjectDelay(PhysicsProcessProcesses[coindex.I],
                                                                      PhysicsProcessProcesses[coindex.I].Current -
                                                                      GetSegmentTime(coindex.Seg));
                }

                return isPaused;
            case Segment.DeferredProcess:
                isPaused = DeferredProcessPaused[coindex.I];
                DeferredProcessPaused[coindex.I] = newPausedState;

                if (newPausedState && DeferredProcessProcesses[coindex.I].Current > GetSegmentTime(coindex.Seg))
                {
                    DeferredProcessProcesses[coindex.I] = _InjectDelay(DeferredProcessProcesses[coindex.I],
                                                                       DeferredProcessProcesses[coindex.I].Current -
                                                                       GetSegmentTime(coindex.Seg));
                }

                return isPaused;
            default:
                return false;
        }
    }

    private bool SetHeld(ProcessIndex coindex, bool newHeldState)
    {
        if (CoindexPeek(coindex) == null)
        {
            return false;
        }

        bool isHeld;

        switch (coindex.Seg)
        {
            case Segment.Process:
                isHeld = ProcessHeld[coindex.I];
                ProcessHeld[coindex.I] = newHeldState;

                if (newHeldState && ProcessProcesses[coindex.I].Current > GetSegmentTime(coindex.Seg))
                {
                    ProcessProcesses[coindex.I] = _InjectDelay(ProcessProcesses[coindex.I],
                                                               ProcessProcesses[coindex.I].Current -
                                                               GetSegmentTime(coindex.Seg));
                }

                return isHeld;
            case Segment.PhysicsProcess:
                isHeld = PhysicsProcessHeld[coindex.I];
                PhysicsProcessHeld[coindex.I] = newHeldState;

                if (newHeldState && PhysicsProcessProcesses[coindex.I].Current > GetSegmentTime(coindex.Seg))
                {
                    PhysicsProcessProcesses[coindex.I] = _InjectDelay(PhysicsProcessProcesses[coindex.I],
                                                                      PhysicsProcessProcesses[coindex.I].Current -
                                                                      GetSegmentTime(coindex.Seg));
                }

                return isHeld;
            case Segment.DeferredProcess:
                isHeld = DeferredProcessHeld[coindex.I];
                DeferredProcessHeld[coindex.I] = newHeldState;

                if (newHeldState && DeferredProcessProcesses[coindex.I].Current > GetSegmentTime(coindex.Seg))
                {
                    DeferredProcessProcesses[coindex.I] = _InjectDelay(DeferredProcessProcesses[coindex.I],
                                                                       DeferredProcessProcesses[coindex.I].Current -
                                                                       GetSegmentTime(coindex.Seg));
                }

                return isHeld;
            default:
                return false;
        }
    }

    private IEnumerator<double> _InjectDelay(IEnumerator<double> proc, double delayTime)
    {
        yield return WaitForSecondsOnInstance(delayTime);

        _tmpRef = proc;
        ReplacementFunction = ReturnTmpRefForRepFunc;

        yield return double.NaN;
    }

    private bool CoindexIsPaused(ProcessIndex coindex)
    {
        switch (coindex.Seg)
        {
            case Segment.Process:
                return ProcessPaused[coindex.I];
            case Segment.PhysicsProcess:
                return PhysicsProcessPaused[coindex.I];
            case Segment.DeferredProcess:
                return DeferredProcessPaused[coindex.I];
            default:
                return false;
        }
    }

    private bool CoindexIsHeld(ProcessIndex coindex)
    {
        switch (coindex.Seg)
        {
            case Segment.Process:
                return ProcessHeld[coindex.I];
            case Segment.PhysicsProcess:
                return PhysicsProcessHeld[coindex.I];
            case Segment.DeferredProcess:
                return DeferredProcessHeld[coindex.I];
            default:
                return false;
        }
    }

    private void CoIndexReplace(ProcessIndex coIndex, IEnumerator<double> replacement)
    {
        switch (coIndex.Seg)
        {
            case Segment.Process:
                ProcessProcesses[coIndex.I] = replacement;

                return;
            case Segment.PhysicsProcess:
                PhysicsProcessProcesses[coIndex.I] = replacement;

                return;
            case Segment.DeferredProcess:
                DeferredProcessProcesses[coIndex.I] = replacement;

                return;
        }
    }

    /// <summary>
    ///     Use "yield return Timing.WaitForSeconds(time);" to wait for the specified number of seconds.
    /// </summary>
    /// <param name="waitTime">Number of seconds to wait.</param>
    public static double WaitForSeconds(double waitTime)
    {
        if (double.IsNaN(waitTime))
        {
            waitTime = 0f;
        }

        return Instance.localTime + waitTime;
    }

    /// <summary>
    ///     Use "yield return timingInstance.WaitForSecondsOnInstance(time);" to wait for the specified number of seconds.
    /// </summary>
    /// <param name="waitTime">Number of seconds to wait.</param>
    public double WaitForSecondsOnInstance(double waitTime)
    {
        if (double.IsNaN(waitTime))
        {
            waitTime = 0f;
        }

        return localTime + waitTime;
    }

    /// <summary>
    ///     Use the command "yield return Timing.WaitUntilDone(otherCoroutine);" to pause the current
    ///     coroutine until otherCoroutine is done.
    /// </summary>
    /// <param name="otherCoroutine">The coroutine to pause for.</param>
    public static double WaitUntilDone(CoroutineHandle otherCoroutine) => WaitUntilDone(otherCoroutine, true);

    /// <summary>
    ///     Use the command "yield return Timing.WaitUntilDone(otherCoroutine, false);" to pause the current
    ///     coroutine until otherCoroutine is done, supressing warnings.
    /// </summary>
    /// <param name="otherCoroutine">The coroutine to pause for.</param>
    /// <param name="warnOnIssue">Post a warning to the console if no hold action was actually performed.</param>
    public static double WaitUntilDone(CoroutineHandle otherCoroutine, bool warnOnIssue)
    {
        var inst = GetInstance(otherCoroutine.Key);

        if (inst != null && inst._handleToIndex.ContainsKey(otherCoroutine))
        {
            if (inst.CoindexIsNull(inst._handleToIndex[otherCoroutine]))
            {
                return 0f;
            }

            if (!inst._waitingTriggers.ContainsKey(otherCoroutine))
            {
                inst.CoIndexReplace(inst._handleToIndex[otherCoroutine],
                                    inst._StartWhenDone(otherCoroutine,
                                                        inst.CoindexPeek(inst._handleToIndex[otherCoroutine])));
                inst._waitingTriggers.Add(otherCoroutine, new HashSet<CoroutineHandle>());
            }

            if (inst.currentCoroutine == otherCoroutine)
            {
                if (warnOnIssue)
                {
                    GD.PrintErr("A coroutine cannot wait for itself.");
                }

                return WaitForOneFrame;
            }

            if (!inst.currentCoroutine.IsValid)
            {
                if (warnOnIssue)
                {
                    GD.PrintErr("The two coroutines are not running on the same MEC instance.");
                }

                return WaitForOneFrame;
            }

            inst._waitingTriggers[otherCoroutine].Add(inst.currentCoroutine);
            inst._allWaiting.Add(inst.currentCoroutine);

            inst.SetHeld(inst._handleToIndex[inst.currentCoroutine], true);
            inst.SwapToLast(otherCoroutine, inst.currentCoroutine);

            return double.NaN;
        }

        if (warnOnIssue)
        {
            GD.PrintErr("WaitUntilDone cannot hold: The coroutine handle that was passed in is invalid.\n" +
                        otherCoroutine);
        }

        return WaitForOneFrame;
    }

    private IEnumerator<double> _StartWhenDone(CoroutineHandle handle, IEnumerator<double> proc)
    {
        if (!_waitingTriggers.ContainsKey(handle))
        {
            yield break;
        }

        try
        {
            if (proc.Current > localTime)
            {
                yield return proc.Current;
            }

            while (proc.MoveNext())
            {
                yield return proc.Current;
            }
        }
        finally
        {
            CloseWaitingProcess(handle);
        }
    }

    private void SwapToLast(CoroutineHandle firstHandle, CoroutineHandle lastHandle)
    {
        if (firstHandle.Key != lastHandle.Key)
        {
            return;
        }

        var firstIndex = _handleToIndex[firstHandle];
        var lastIndex = _handleToIndex[lastHandle];

        if (firstIndex.Seg != lastIndex.Seg || firstIndex.I < lastIndex.I)
        {
            return;
        }

        var tempCoptr = CoindexPeek(firstIndex);
        CoIndexReplace(firstIndex, CoindexPeek(lastIndex));
        CoIndexReplace(lastIndex, tempCoptr);

        _indexToHandle[firstIndex] = lastHandle;
        _indexToHandle[lastIndex] = firstHandle;
        _handleToIndex[firstHandle] = lastIndex;
        _handleToIndex[lastHandle] = firstIndex;
        var tmpB = SetPause(firstIndex, CoindexIsPaused(lastIndex));
        SetPause(lastIndex, tmpB);
        tmpB = SetHeld(firstIndex, CoindexIsHeld(lastIndex));
        SetHeld(lastIndex, tmpB);

        if (_waitingTriggers.TryGetValue(lastHandle, out var trigger))
        {
            using var trigsEnum = trigger.GetEnumerator();
            while (trigsEnum.MoveNext())
            {
                SwapToLast(lastHandle, trigsEnum.Current);
            }
        }

        if (_allWaiting.Contains(firstHandle))
        {
            using var keyEnum = _waitingTriggers.GetEnumerator();
            while (keyEnum.MoveNext())
            {
                using var valueEnum = keyEnum.Current.Value.GetEnumerator();
                while (valueEnum.MoveNext())
                {
                    if (valueEnum.Current == firstHandle)
                    {
                        SwapToLast(keyEnum.Current.Key, firstHandle);
                    }
                }
            }
        }
    }

    private void CloseWaitingProcess(CoroutineHandle handle)
    {
        if (!_waitingTriggers.ContainsKey(handle))
        {
            return;
        }

        using var tasksEnum = _waitingTriggers[handle].GetEnumerator();
        _waitingTriggers.Remove(handle);

        while (tasksEnum.MoveNext())
        {
            if (_handleToIndex.ContainsKey(tasksEnum.Current) && !HandleIsInWaitingList(tasksEnum.Current))
            {
                SetHeld(_handleToIndex[tasksEnum.Current], false);
                _allWaiting.Remove(tasksEnum.Current);
            }
        }
    }

    private bool HandleIsInWaitingList(CoroutineHandle handle)
    {
        using var triggersEnum = _waitingTriggers.GetEnumerator();
        while (triggersEnum.MoveNext())
        {
            if (triggersEnum.Current.Value.Contains(handle))
            {
                return true;
            }
        }

        return false;
    }

    private static IEnumerator<double>? ReturnTmpRefForRepFunc(IEnumerator<double> coptr, CoroutineHandle handle) =>
        _tmpRef as IEnumerator<double>;

    /// <summary>
    ///     Keeps this coroutine from executing until UnlockCoroutine is called with a matching key.
    /// </summary>
    /// <param name="coroutine">The handle to the coroutine to be locked.</param>
    /// <param name="key">The key to use. A new key can be generated by calling "new CoroutineHandle(0)".</param>
    /// <returns>Whether the lock was successful.</returns>
    public bool LockCoroutine(CoroutineHandle coroutine, CoroutineHandle key)
    {
        if (coroutine.Key != _instanceID || key == new CoroutineHandle() || key.Key != 0)
        {
            return false;
        }

        if (!_waitingTriggers.ContainsKey(key))
        {
            _waitingTriggers.Add(key, new HashSet<CoroutineHandle> { coroutine });
        }
        else
        {
            _waitingTriggers[key].Add(coroutine);
        }

        _allWaiting.Add(coroutine);

        SetHeld(_handleToIndex[coroutine], true);

        return true;
    }

    /// <summary>
    ///     Unlocks a coroutine that has been locked, so long as the key matches.
    /// </summary>
    /// <param name="coroutine">The handle to the coroutine to be unlocked.</param>
    /// <param name="key">The key that the coroutine was previously locked with.</param>
    /// <returns>Whether the coroutine was successfully unlocked.</returns>
    public bool UnlockCoroutine(CoroutineHandle coroutine, CoroutineHandle key)
    {
        if (coroutine.Key != _instanceID || key == new CoroutineHandle() ||
            !_handleToIndex.ContainsKey(coroutine) || !_waitingTriggers.ContainsKey(key))
        {
            return false;
        }

        if (_waitingTriggers[key].Count == 1)
        {
            _waitingTriggers.Remove(key);
        }
        else
        {
            _waitingTriggers[key].Remove(coroutine);
        }

        if (!HandleIsInWaitingList(coroutine))
        {
            SetHeld(_handleToIndex[coroutine], false);
            _allWaiting.Remove(coroutine);
        }

        return true;
    }

    /// <summary>
    ///     Calls the specified action after current process step is completed.
    /// </summary>
    /// <param name="action">The action to call.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public static CoroutineHandle CallDeferred(Action? action) =>
        action == null
            ? new CoroutineHandle()
            : RunCoroutine(Instance._DelayedCall(0, action), Segment.DeferredProcess);

    /// <summary>
    ///     Calls the specified action after current process step is completed.
    /// </summary>
    /// <param name="action">The action to call.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public CoroutineHandle CallDeferredOnInstance(Action? action) =>
        action == null
            ? new CoroutineHandle()
            : RunCoroutine(_DelayedCall(0, action), Segment.DeferredProcess);

    /// <summary>
    ///     Calls the specified action after a specified number of seconds.
    /// </summary>
    /// <param name="delay">The number of seconds to wait before calling the action.</param>
    /// <param name="action">The action to call.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public static CoroutineHandle CallDelayed(double delay, Action? action) =>
        action == null ? new CoroutineHandle() : RunCoroutine(Instance._DelayedCall(delay, action));

    /// <summary>
    ///     Calls the specified action after a specified number of seconds.
    /// </summary>
    /// <param name="delay">The number of seconds to wait before calling the action.</param>
    /// <param name="action">The action to call.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public CoroutineHandle CallDelayedOnInstance(double delay, Action? action) =>
        action == null ? new CoroutineHandle() : RunCoroutineOnInstance(_DelayedCall(delay, action));

    /// <summary>
    ///     Calls the specified action after a specified number of seconds.
    /// </summary>
    /// <param name="delay">The number of seconds to wait before calling the action.</param>
    /// <param name="action">The action to call.</param>
    /// <param name="segment">The timing segment that the call should be made in.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public static CoroutineHandle CallDelayed(double delay, Segment segment, Action? action) =>
        action == null
            ? new CoroutineHandle()
            : RunCoroutine(Instance._DelayedCall(delay, action), segment);

    /// <summary>
    ///     Calls the specified action after a specified number of seconds.
    /// </summary>
    /// <param name="delay">The number of seconds to wait before calling the action.</param>
    /// <param name="action">The action to call.</param>
    /// <param name="segment">The timing segment that the call should be made in.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public CoroutineHandle CallDelayedOnInstance(double delay, Segment segment, Action? action) =>
        action == null
            ? new CoroutineHandle()
            : RunCoroutineOnInstance(_DelayedCall(delay, action), segment);

    private IEnumerator<double> _DelayedCall(double delay, Action? action)
    {
        if (action is null)
        {
            yield break;
        }

        yield return WaitForSecondsOnInstance(delay);
        action();
    }

    /// <summary>
    ///     Calls the supplied action at the given rate for a given number of seconds.
    /// </summary>
    /// <param name="timeframe">The number of seconds that this function should run.</param>
    /// <param name="period">The amount of time between calls.</param>
    /// <param name="action">The action to call every frame.</param>
    /// <param name="onDone">An optional action to call when this function finishes.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public static CoroutineHandle CallPeriodically(double timeframe, double period, Action? action,
                                                   Action? onDone = null) =>
        action == null
            ? new CoroutineHandle()
            : RunCoroutine(Instance._CallContinuously(timeframe, period, action, onDone), Segment.Process);

    /// <summary>
    ///     Calls the supplied action at the given rate for a given number of seconds.
    /// </summary>
    /// <param name="timeframe">The number of seconds that this function should run.</param>
    /// <param name="period">The amount of time between calls.</param>
    /// <param name="action">The action to call every frame.</param>
    /// <param name="onDone">An optional action to call when this function finishes.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public CoroutineHandle CallPeriodicallyOnInstance(double timeframe, double period, Action? action,
                                                      Action? onDone = null) =>
        action == null
            ? new CoroutineHandle()
            : RunCoroutineOnInstance(_CallContinuously(timeframe, period, action, onDone), Segment.Process);

    /// <summary>
    ///     Calls the supplied action at the given rate for a given number of seconds.
    /// </summary>
    /// <param name="timeframe">The number of seconds that this function should run.</param>
    /// <param name="period">The amount of time between calls.</param>
    /// <param name="action">The action to call every frame.</param>
    /// <param name="segment">The timing segment to run in.</param>
    /// <param name="onDone">An optional action to call when this function finishes.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public static CoroutineHandle CallPeriodically(double timeframe, double period, Action? action, Segment segment,
                                                   Action? onDone = null) =>
        action == null
            ? new CoroutineHandle()
            : RunCoroutine(Instance._CallContinuously(timeframe, period, action, onDone), segment);

    /// <summary>
    ///     Calls the supplied action at the given rate for a given number of seconds.
    /// </summary>
    /// <param name="timeframe">The number of seconds that this function should run.</param>
    /// <param name="period">The amount of time between calls.</param>
    /// <param name="action">The action to call every frame.</param>
    /// <param name="segment">The timing segment to run in.</param>
    /// <param name="onDone">An optional action to call when this function finishes.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public CoroutineHandle CallPeriodicallyOnInstance(double timeframe, double period, Action? action, Segment segment,
                                                      Action? onDone = null) =>
        action == null
            ? new CoroutineHandle()
            : RunCoroutineOnInstance(_CallContinuously(timeframe, period, action, onDone), segment);

    /// <summary>
    ///     Calls the supplied action at the given rate for a given number of seconds.
    /// </summary>
    /// <param name="timeframe">The number of seconds that this function should run.</param>
    /// <param name="action">The action to call every frame.</param>
    /// <param name="onDone">An optional action to call when this function finishes.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public static CoroutineHandle CallContinuously(double timeframe, Action? action, Action? onDone = null) =>
        action == null
            ? new CoroutineHandle()
            : RunCoroutine(Instance._CallContinuously(timeframe, 0f, action, onDone), Segment.Process);

    /// <summary>
    ///     Calls the supplied action at the given rate for a given number of seconds.
    /// </summary>
    /// <param name="timeframe">The number of seconds that this function should run.</param>
    /// <param name="action">The action to call every frame.</param>
    /// <param name="onDone">An optional action to call when this function finishes.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public CoroutineHandle CallContinuouslyOnInstance(double timeframe, Action? action, Action? onDone = null) =>
        action == null
            ? new CoroutineHandle()
            : RunCoroutineOnInstance(_CallContinuously(timeframe, 0f, action, onDone), Segment.Process);

    /// <summary>
    ///     Calls the supplied action every frame for a given number of seconds.
    /// </summary>
    /// <param name="timeframe">The number of seconds that this function should run.</param>
    /// <param name="action">The action to call every frame.</param>
    /// <param name="timing">The timing segment to run in.</param>
    /// <param name="onDone">An optional action to call when this function finishes.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public static CoroutineHandle CallContinuously(double timeframe, Action? action, Segment timing,
                                                   Action? onDone = null) =>
        action == null
            ? new CoroutineHandle()
            : RunCoroutine(Instance._CallContinuously(timeframe, 0f, action, onDone), timing);

    /// <summary>
    ///     Calls the supplied action every frame for a given number of seconds.
    /// </summary>
    /// <param name="timeframe">The number of seconds that this function should run.</param>
    /// <param name="action">The action to call every frame.</param>
    /// <param name="timing">The timing segment to run in.</param>
    /// <param name="onDone">An optional action to call when this function finishes.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public CoroutineHandle CallContinuouslyOnInstance(double timeframe, Action? action, Segment timing,
                                                      Action? onDone = null) =>
        action == null
            ? new CoroutineHandle()
            : RunCoroutineOnInstance(_CallContinuously(timeframe, 0f, action, onDone), timing);

    private IEnumerator<double> _CallContinuously(double timeframe, double period, Action action, Action? onDone)
    {
        var startTime = localTime;
        while (localTime <= startTime + timeframe)
        {
            yield return WaitForSecondsOnInstance(period);

            action();
        }

        onDone?.Invoke();
    }

    /// <summary>
    ///     Calls the supplied action at the given rate for a given number of seconds.
    /// </summary>
    /// <param name="reference">A value that will be passed in to the supplied action each period.</param>
    /// <param name="timeframe">The number of seconds that this function should run.</param>
    /// <param name="period">The amount of time between calls.</param>
    /// <param name="action">The action to call every frame.</param>
    /// <param name="onDone">An optional action to call when this function finishes.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public static CoroutineHandle CallPeriodically<T>
        (T reference, double timeframe, double period, Action<T>? action, Action<T>? onDone = null) =>
        action == null
            ? new CoroutineHandle()
            : RunCoroutine(Instance._CallContinuously(reference, timeframe, period, action, onDone), Segment.Process);

    /// <summary>
    ///     Calls the supplied action at the given rate for a given number of seconds.
    /// </summary>
    /// <param name="reference">A value that will be passed in to the supplied action each period.</param>
    /// <param name="timeframe">The number of seconds that this function should run.</param>
    /// <param name="period">The amount of time between calls.</param>
    /// <param name="action">The action to call every frame.</param>
    /// <param name="onDone">An optional action to call when this function finishes.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public CoroutineHandle CallPeriodicallyOnInstance<T>
        (T reference, double timeframe, double period, Action<T>? action, Action<T>? onDone = null) =>
        action == null
            ? new CoroutineHandle()
            : RunCoroutineOnInstance(_CallContinuously(reference, timeframe, period, action, onDone), Segment.Process);

    /// <summary>
    ///     Calls the supplied action at the given rate for a given number of seconds.
    /// </summary>
    /// <param name="reference">A value that will be passed in to the supplied action each period.</param>
    /// <param name="timeframe">The number of seconds that this function should run.</param>
    /// <param name="period">The amount of time between calls.</param>
    /// <param name="action">The action to call every frame.</param>
    /// <param name="timing">The timing segment to run in.</param>
    /// <param name="onDone">An optional action to call when this function finishes.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public static CoroutineHandle CallPeriodically<T>(T reference, double timeframe, double period, Action<T>? action,
                                                      Segment timing, Action<T>? onDone = null) =>
        action == null
            ? new CoroutineHandle()
            : RunCoroutine(Instance._CallContinuously(reference, timeframe, period, action, onDone), timing);

    /// <summary>
    ///     Calls the supplied action at the given rate for a given number of seconds.
    /// </summary>
    /// <param name="reference">A value that will be passed in to the supplied action each period.</param>
    /// <param name="timeframe">The number of seconds that this function should run.</param>
    /// <param name="period">The amount of time between calls.</param>
    /// <param name="action">The action to call every frame.</param>
    /// <param name="timing">The timing segment to run in.</param>
    /// <param name="onDone">An optional action to call when this function finishes.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public CoroutineHandle CallPeriodicallyOnInstance<T>(T reference, double timeframe, double period,
                                                         Action<T>? action,
                                                         Segment timing, Action<T>? onDone = null) =>
        action == null
            ? new CoroutineHandle()
            : RunCoroutineOnInstance(_CallContinuously(reference, timeframe, period, action, onDone), timing);

    /// <summary>
    ///     Calls the supplied action every frame for a given number of seconds.
    /// </summary>
    /// <param name="reference">A value that will be passed in to the supplied action each frame.</param>
    /// <param name="timeframe">The number of seconds that this function should run.</param>
    /// <param name="action">The action to call every frame.</param>
    /// <param name="onDone">An optional action to call when this function finishes.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public static CoroutineHandle CallContinuously<T>(T reference, double timeframe, Action<T>? action,
                                                      Action<T>? onDone = null) =>
        action == null
            ? new CoroutineHandle()
            : RunCoroutine(Instance._CallContinuously(reference, timeframe, 0f, action, onDone), Segment.Process);

    /// <summary>
    ///     Calls the supplied action every frame for a given number of seconds.
    /// </summary>
    /// <param name="reference">A value that will be passed in to the supplied action each frame.</param>
    /// <param name="timeframe">The number of seconds that this function should run.</param>
    /// <param name="action">The action to call every frame.</param>
    /// <param name="onDone">An optional action to call when this function finishes.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public CoroutineHandle CallContinuouslyOnInstance<T>(T reference, double timeframe, Action<T>? action,
                                                         Action<T>? onDone = null) =>
        action == null
            ? new CoroutineHandle()
            : RunCoroutineOnInstance(_CallContinuously(reference, timeframe, 0f, action, onDone), Segment.Process);

    /// <summary>
    ///     Calls the supplied action every frame for a given number of seconds.
    /// </summary>
    /// <param name="reference">A value that will be passed in to the supplied action each frame.</param>
    /// <param name="timeframe">The number of seconds that this function should run.</param>
    /// <param name="action">The action to call every frame.</param>
    /// <param name="timing">The timing segment to run in.</param>
    /// <param name="onDone">An optional action to call when this function finishes.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public static CoroutineHandle CallContinuously<T>(T reference, double timeframe, Action<T>? action,
                                                      Segment timing, Action<T>? onDone = null) =>
        action == null
            ? new CoroutineHandle()
            : RunCoroutine(Instance._CallContinuously(reference, timeframe, 0f, action, onDone), timing);

    /// <summary>
    ///     Calls the supplied action every frame for a given number of seconds.
    /// </summary>
    /// <param name="reference">A value that will be passed in to the supplied action each frame.</param>
    /// <param name="timeframe">The number of seconds that this function should run.</param>
    /// <param name="action">The action to call every frame.</param>
    /// <param name="timing">The timing segment to run in.</param>
    /// <param name="onDone">An optional action to call when this function finishes.</param>
    /// <returns>The handle to the coroutine that is started by this function.</returns>
    public CoroutineHandle CallContinuouslyOnInstance<T>(T reference, double timeframe, Action<T>? action,
                                                         Segment timing, Action<T>? onDone = null) =>
        action == null
            ? new CoroutineHandle()
            : RunCoroutineOnInstance(_CallContinuously(reference, timeframe, 0f, action, onDone), timing);

    private IEnumerator<double> _CallContinuously<T>(T reference, double timeframe, double period,
                                                     Action<T> action, Action<T>? onDone = null)
    {
        var startTime = localTime;
        while (localTime <= startTime + timeframe)
        {
            yield return WaitForSecondsOnInstance(period);

            action(reference);
        }

        onDone?.Invoke(reference);
    }

    private struct ProcessIndex : IEquatable<ProcessIndex>
    {
        public Segment Seg;
        public int I;

        public bool Equals(ProcessIndex other) => Seg == other.Seg && I == other.I;

        public override bool Equals(object? other)
        {
            if (other is ProcessIndex index)
            {
                return Equals(index);
            }

            return false;
        }

        public static bool operator ==(ProcessIndex a, ProcessIndex b) => a.Seg == b.Seg && a.I == b.I;

        public static bool operator !=(ProcessIndex a, ProcessIndex b) => a.Seg != b.Seg || a.I != b.I;

        public override int GetHashCode() => ((int) Seg - 2) * (int.MaxValue / 3) + I;
    }
}
