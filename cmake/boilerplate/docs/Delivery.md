# Shaders, plugins and application delivery

These helpers are included by `Boilerplate.cmake`. Ordinary inclusion does not
search for shader compilers or deployment tools. All workflows use native CMake;
Python is not required.

## Shader compilation

```cmake
boilerplate_add_executable(viewer SOURCES main.cpp)
boilerplate_add_glsl_shaders(viewer
  NAME forward
  SOURCE_DIR shaders
  SOURCES mesh.vert mesh.frag
  INCLUDE_DIRS shaders/include
  DEFINES LIGHT_COUNT=4
  COPY_TO_RUNTIME)
boilerplate_add_glsl_shaders(viewer
  NAME shadow
  SOURCE_DIR shaders
  SOURCES mesh.vert
  INCLUDE_DIRS shaders/include
  DEFINES SHADOW_PASS=1
  COPY_TO_RUNTIME)

boilerplate_add_hlsl_shaders(viewer
  NAME tonemap
  SOURCE_DIR shaders
  SOURCES tonemap.hlsl
  PROFILE ps_6_0 ENTRY_POINT main
  FORMAT DXIL # or SPIRV, with a DXC build supporting SPIR-V
  COPY_TO_RUNTIME)
```

Each call declares a named variant. A name must be unique within a target.
`boilerplate_add_shaders()` is the common API with `LANGUAGE GLSL|HLSL`.
GLSL prefers `glslc`, then `glslangValidator`; HLSL uses `dxc`.
`COMPILER` selects a compiler path/name; `BACKEND glslc|glslang|dxc` is useful for
wrappers or explicitly selecting a GLSL backend. The GLSL wrapper also honors
`Vulkan_GLSLC_EXECUTABLE` when no backend/compiler is explicitly selected.
GLSL can discover supported shader suffixes recursively when `SOURCES` is omitted;
HLSL requires explicit sources sharing the specified profile and entry point.

`DEFINES`, `INCLUDE_DIRS` and `OPTIONS` are lists. `DEPENDS` accepts extra files or
generator targets, such as a target producing an include before the first build.
Compiler-generated depfiles track transitive includes on subsequent builds.
Known absolute source/include prefixes are escaped for directories containing
spaces. Keep transitive include filenames themselves free of whitespace: shader
compiler depfile formats do not consistently preserve those filename boundaries.
DXC needs separate dependency-generation and compilation invocations.
See the [DXC option definitions](https://github.com/microsoft/DirectXShaderCompiler/blob/main/include/dxc/Support/HLSLOptions.td).

Outputs are `<OUTPUT_DIR>/<config>/<variant>/<source-relative-path>.spv|.dxil`;
the default output directory is `<binary-dir>/shaders/<target>`. The unnamed
`default` variant omits its variant subdirectory. Debug and Release have separate
outputs. Files outside `SOURCE_DIR` and duplicate output paths are rejected.
The helper appends outputs to `BOILERPLATE_SHADER_OUTPUTS` on the owner target;
use `TARGET_GENEX_EVAL` when evaluating that property inside generator expressions.

Building the owner or `<target>_shaders` builds all variants. With `COPY_TO_RUNTIME`,
copies under `<executable-dir>/shaders/<variant>` are refreshed even when no C++
file changed; deleted copies are restored on the next build. Register shaders
before calling `boilerplate_install_application()` to install their binaries too.

## Plugins and ABI

```cmake
boilerplate_add_plugin(renderer_plugin
  SOURCES plugin.cpp
  INCLUDE_DIRS include
  LIBRARIES renderer_core::renderer_core
  POLICIES minimal hardening)
```

This creates a `MODULE` library with hidden visibility, PIC and the normal target
policy machinery. Include the generated `renderer_plugin_export.h` and mark the
C entry point with `RENDERER_PLUGIN_EXPORT`. The binary has no `lib` prefix and
uses the platform's module suffix. Windows build-tree DLL dependencies known to
CMake are copied beside the plugin. Load the module at runtime; do not link a
host against a `MODULE` target.

`examples/plugins` provides a host using `LoadLibraryExW`/`dlopen`, symbol lookup,
ABI version/size validation, and a reload cycle. The contract passes fixed-width
values through one C entry point and a function table; it passes no STL objects,
exceptions or allocations across the boundary. The host retains state, finishes
calls, discards all plugin pointers, unloads, then loads the next generation.

```sh
cmake -S . -B out/build/plugins -G Ninja -DBOILERPLATE_BUILD_EXAMPLES=ON
cmake --build out/build/plugins --target plugin_host
ctest --test-dir out/build/plugins -R plugin_reload --output-on-failure
# The host accepts: plugin_host plugin [replacement-plugin] [resource-file]
```

This is an explicit reload example, not a background file watcher or an engine
state-migration system. A threaded host must stop calls and join plugin workers
before unloading; plugin-owned objects must be destroyed while its code is loaded.
Pass a different file path to load a replacement binary. Platform/architecture and
calling conventions still must match even with a C ABI.

CFI applies within its link unit. `cfi-icall` on a host calling arbitrary plugin
function pointers is not automatically compatible with a dynamically loaded module.
The example uses `minimal`; choose an explicit boundary strategy before applying
CFI to a real plugin host. The plugin helper rejects diagnostic CFI because its
UBSan runtime would need explicit integration with the host; trap-mode internal
CFI and Windows CFG remain available.

## Installed application layout

```cmake
boilerplate_add_executable(editor SOURCES main.cpp LIBRARIES engine_core::engine_core)
# Register shaders here, if any.
boilerplate_install_application(editor
  LIBRARIES engine_core::shared renderer_core::shared
  PLUGINS renderer_plugin
  RESOURCE_DIRS assets
  RUNTIME_DIRS "${vendor_runtime_directory}")
```

Do not also request the simple `INSTALL` option on the executable: this helper
owns its installation. All destinations follow relative `GNUInstallDirs` paths:

| Artifact | Destination under the installation prefix |
| --- | --- |
| Executable and dependent Windows DLLs | `bin/` |
| Shared libraries on Linux/macOS | `lib/` (or configured `CMAKE_INSTALL_LIBDIR`) |
| Plugins | `lib/<target>/plugins/` |
| Contents of resource directories | `share/<target>/` |
| Compiled shader variants | `share/<target>/shaders/` |

`LIBRARIES` explicitly lists owned shared dependencies, including transitive ones;
aliases are accepted. Static libraries need no runtime installation. `PLUGINS`
lists modules loaded dynamically. External dependencies of all registered binaries
are collected using CMake's native runtime dependency scanner. Imported shared
targets may also be listed explicitly. OS libraries are excluded by default;
`PRE_EXCLUDE_REGEXES` and `POST_EXCLUDE_REGEXES` extend the filters, and
`RUNTIME_DIRS` provides search directories. Missing/conflicting dependencies fail
installation. See [CMake runtime dependency sets](https://cmake.org/cmake/help/latest/command/install.html#runtime-dependency-set).

On Linux, runtime collection requires `patchelf`: installed libraries receive
`$ORIGIN` search paths so transitive third-party dependencies resolve after moving
the package. Only files recorded in the current installation manifest are modified.
Owned executables/plugins receive relative install RPATHs. macOS external libraries
must already use relocatable `@rpath`/`@loader_path` install names; arbitrary vendor
framework rewriting, app bundles, signing and notarization are outside this helper.
Qt applications should also use `boilerplate_enable_qt_deployment()`.

`NO_RUNTIME_DEPENDENCIES` disables external scanning and fixup; explicit target and
resource installation still works. Cross compilation requires this option and a
target-specific dependency packaging setup. `BOILERPLATE_INSTALL=OFF` disables
installation. Resources are copied during install, so changing them needs only a
new install. Build before installing; `cmake --install` does not build targets.

```sh
cmake --build out/build/plugins --parallel
cmake --install out/build/plugins --prefix out/stage --component Runtime
```

The same Runtime rules are used by CPack. Resolve resource/plugin locations from
the executable or explicit application settings, not the working directory.
The helper exposes `BOILERPLATE_INSTALL_PLUGIN_DIR` and
`BOILERPLATE_INSTALL_RESOURCE_DIR` target properties for project-generated config.

## Regression checks

```sh
cmake --build out/build/plugins --target boilerplate_check_delivery
cmake --build out/build/plugins --target boilerplate_check_shaders
```

Both checks exercise Ninja and Ninja Multi-Config. Delivery checks ABI mismatch,
missing entry point, reload, owned and transitive external libraries, resources,
and launching a relocated installation after deleting the original build trees.
Shader checks use real available compilers in paths containing spaces and exercise named variants, nested
include edits, no-op builds, runtime copy recovery, install and configuration
separation. A missing backend is reported as skipped; no available backend fails.
For strict checks, run `verify_shaders.cmake` with `REQUIRE_ALL_BACKENDS=ON`.
Linux CI installs all three compilers; Windows/macOS CI runs native delivery checks.
