# Argument contract shared by setup.sh and build.sh. Bash 3.2 compatible.
IFS= read -r preset < "$root/scripts/default-preset.txt"
profile=minimal tools_dir="$root/out/host-tools" jobs=2 run_target=
run_application=false run_tests=true install_artifacts=false package_artifacts=false package_format=
dry_run=false setup_only=false host_extra=() build_args=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --preset|--profile|--tools-dir|--jobs|--run-target|--vcpkg-root|--manager|--package-format)
      [[ $# -ge 2 && -n "$2" && "$2" != --* ]] || { echo "Missing value for $1" >&2; exit 2; }
      case "$1" in
        --preset) preset="$2";; --profile) profile="$2";; --tools-dir) tools_dir="$2";;
        --jobs) jobs="$2";; --run-target) run_target="$2";;
        --package-format) package_format="$2"; package_artifacts=true;;
        *) host_extra+=("$1" "$2");;
      esac
      shift 2;;
    --run) run_application=true; shift;;
    --no-tests) run_tests=false; shift;;
    --install-artifacts) install_artifacts=true; shift;;
    --package) package_artifacts=true; shift;;
    --dry-run) dry_run=true; shift;;
    --setup-only) setup_only=true; shift;;
    --help|-h)
      echo "Usage: $entry [--preset NAME] [--jobs N] [--run] [--run-target TARGET] [--no-tests] [--install-artifacts] [--package [--package-format FORMAT]] [--dry-run]"
      echo 'Tools: --profile minimal|package|dev|ci --setup-only --tools-dir PATH --vcpkg-root PATH'
      echo 'One invocation prepares DagFlow tools, configures, builds and runs CTest.'
      echo 'The --run flag launches the basic DagFlow example; --package creates platform-native distributables under out/packages/.'
      exit 0;;
    *) echo "Unknown argument: $1" >&2; exit 2;;
  esac
done
[[ "$preset" =~ ^[a-zA-Z0-9_-]+$ && "$jobs" =~ ^[1-9][0-9]*$ ]] || { echo 'Invalid preset or jobs' >&2; exit 2; }
[[ -z "$run_target" || "$run_target" =~ ^[a-zA-Z0-9_.+-]+$ ]] || { echo 'Invalid run target' >&2; exit 2; }
case "$profile" in minimal|package|dev|ci) ;; *) echo 'Invalid tool profile' >&2; exit 2;; esac
[[ -z "$package_format" || "$package_format" =~ ^(TGZ|ZIP|DEB|RPM|NSIS|DragNDrop|ARCH)$ ]] || { echo "Unsupported package format: $package_format" >&2; exit 2; }
if [[ "$package_artifacts" == true && "$profile" == minimal ]]; then profile=package; fi
build_args=(--preset "$preset" --jobs "$jobs" --tools-dir "$tools_dir")
[[ -z "$run_target" ]] || build_args+=(--run-target "$run_target")
[[ "$run_application" == false ]] || build_args+=(--run)
[[ "$run_tests" == true ]] || build_args+=(--no-tests)
[[ "$install_artifacts" == false ]] || build_args+=(--install-artifacts)
[[ "$package_artifacts" == false ]] || build_args+=(--package)
[[ -z "$package_format" ]] || build_args+=(--package-format "$package_format")
[[ "$dry_run" == false ]] || build_args+=(--dry-run)
