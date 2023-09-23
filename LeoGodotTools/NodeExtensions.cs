using Godot;
using LeoGodotTools.Timing;

namespace LeoGodotTools;

public static class NodeExtensions
{
    public static T GetChildNodeOrThrow<T>(this Node node, string path) where T : Node
    {
        var childNode = node.GetNode(path);
        if (childNode is null)
        {
            throw new InvalidOperationException($"Node {path} is null");
        }

        if (childNode is not T t)
        {
            throw new InvalidOperationException($"Node {path} is not of type {typeof(T)}");
        }

        return t;
    }
    
    public static void ExecuteAfterTimeout(this Node _, TimeSpan duration, Action action)
    {
        Timing.Timing.RunCoroutine(Ex(), Segment.DeferredProcess);

        return;

        IEnumerator<double> Ex()
        {
            yield return Timing.Timing.WaitForSeconds(duration.TotalSeconds);
            action();
        }
    }
    
    public static SignalAwaiter CreateDelay(this Node self, TimeSpan duration)
    {
        var seconds = (float) duration.TotalSeconds;

        return self.ToSignal(self.GetTree().CreateTimer(seconds), SceneTreeTimer.SignalName.Timeout);
    }
}
