using Godot;

namespace LeoGodotTools;

public static class MeshInstanceExtensions
{
    public static StandardMaterial3D? GetStandardMaterial(this MeshInstance3D meshInstance) =>
        meshInstance.GetSurfaceOverrideMaterial(0) as StandardMaterial3D;
}
