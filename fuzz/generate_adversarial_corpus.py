"""Generate deterministic seeds for adversarial layers; never run fuzzers.

Only writes named seeds in these new layers. Existing/mutated corpora are neither
deleted nor enumerated. The byte layouts intentionally match each harness so
exception budgets and chunk boundaries are reachable without random discovery.
"""

from pathlib import Path


CORPUS = Path(__file__).resolve().parent / "corpus"


def seed(layer: str, name: str, values: list[int]) -> None:
    data = bytes(values)
    assert 0 < len(data) <= 4096
    destination = CORPUS / layer / name
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(data)


def exceptions() -> None:
    for copyable in (0, 1):
        for mode in range(4):
            sizes = (4, 8) if mode < 2 else (1, 2, 4)
            for count in sizes:
                size_byte = int(count == 4) if mode < 2 else count - 1
                for budget in range(count + 2):
                    seed("exceptions", f"vector-{copyable}-{mode}-{count}-{budget}",
                         [copyable, mode, size_byte, budget])
    for kind in (2, 3, 4):
        for fail in (0, 1):
            for allocation in (0, 1) if kind == 2 else (0,):
                for move in (0, 1) if kind == 2 else (0,):
                    data = [kind, fail]
                    if kind == 2:
                        data += [allocation, move]
                    seed("exceptions", f"callable-{kind}-{fail}-{allocation}-{move}", data + [73])


def ranges() -> None:
    for boundary in range(12):
        for kind in range(5):
            for stealing in (0, 1) if kind != 3 else (0,):
                for mode in range(3):
                    seed("ranges", f"boundary-{boundary}-{kind}-{stealing}-{mode}",
                         [3, 6, boundary, kind, stealing, mode, 4, 63,
                          boundary % 8, 1, 1, 1, 8, 1])
    # Exact allocation cutoff sweep for multi-chunk publication and WS tiles.
    for stealing in (0, 1):
        for budget in range(16):
            seed("ranges", f"allocation-{stealing}-{budget}",
                 [3, 0, 11, 2, stealing, 2, budget, 255, 0, 0, 0, 0, 1])
    for grain in range(8):
        seed("ranges", f"grain-{grain}",
             [0, 0, 11, 1, 1, 0, 0, 255, grain, 0, 0, 0, 0])


def graph_scopes() -> None:
    for boundary in range(12):
        for mode in range(6):
            seed("graph_scope", f"lifetime-{boundary}-{mode}",
                 [3, 6, boundary, mode, 3, 16, 3, 0, 1, 1, 8, 64])
    for mode in range(6):
        seed("graph_scope", f"single-worker-{mode}",
             [0, 0, 11, mode, 3, 16, 0, 0, 0, 0, 0])


def joins() -> None:
    for count in (1, 2, 16):
        for deep in (False, True):
            for concurrent in (0, 1):
                for errors in range(3):
                    for budget in (0, 1, 4, 23):
                        nodes = 64 if deep else 8
                        data = [count - 1, nodes - 1, 7 if deep else 0,
                                255 if deep else 0, concurrent, budget]
                        for i in range(count):
                            data.append(int(errors == 2 or (errors == 1 and i == 0)))
                            if i:
                                data.append(i % 2)
                        for i in range(nodes):
                            fan_in = (0, 1, 2, 16)[i % 4]
                            data.append(fan_in)
                            available = count + i
                            for j in range(fan_in):
                                index = (0, available - 1, available, j % count)[j % 4]
                                data.append(index)
                                if index != available:
                                    data.append(1) # Duplicate valid dependencies.
                        data.append(1) # Duplicate every edge in the deep chain.
                        seed("joins", f"dag-{count}-{int(deep)}-{concurrent}-{errors}-{budget}", data)


def saturation() -> None:
    for workers in (1, 4):
        for shards in (1, 5):
            for batch in range(8):
                for priorities in (1, 2):
                    seed("saturation", f"queues-{workers}-{shards}-{batch}-{priorities}",
                         [0, workers - 1, shards - 1, batch, shards - 1,
                          priorities - 1, 64, 0])
    for batch in (0, 1, 5, 7):
        for priority in (0, 1):
            for flag in (0, 1):
                for other in (0, 1):
                    seed("saturation", f"worker-{batch}-{priority}-{flag}-{other}",
                         [1, batch, priority, flag, 64, other])
                    seed("saturation", f"external-{batch}-{priority}-{flag}-{other}",
                         [2, batch, priority, 64, flag, other])
    for shards in (1, 2, 7):
        for batch in range(8):
            for high in (0, 1):
                for local_normal in (0, 1):
                    seed("saturation", f"fairness-{shards}-{batch}-{high}-{local_normal}",
                         [3, shards - 1, batch, high, local_normal])


def scope_races() -> None:
    for workers in (1, 4):
        cfg = [workers - 1, 5] + [(workers - i) % 6 for i in range(workers)]
        for mode in range(4):
            for depth in (0, 4, 8):
                for delay in (0, 31):
                    seed("scope_races", f"admission-{workers}-{mode}-{depth}-{delay}",
                         [0] + cfg + [2, 15, 7, mode, depth, 1, 1, delay])
        for child in (0, 1):
            for cancel in (0, 1):
                for fail in (0, 1):
                    seed("scope_races", f"reserved-{workers}-{child}-{cancel}-{fail}",
                         [1] + cfg + [child, cancel, fail])
            for budget in range(12):
                seed("scope_races", f"allocation-{workers}-{child}-{budget}",
                     [2] + cfg + [child, budget])
    for depth in range(17):
        seed("scope_races", f"ancestor-{depth}", [3, depth])


def wake_protocols() -> None:
    for workers in (1, 2, 4, 8):
        for shards in (1, 3, 9):
            for batch in (0, 1):
                for clustered in (0, 1):
                    cfg = [workers - 1, shards - 1, batch, clustered]
                    if not clustered:
                        cfg += [(2 * i + 1) % shards for i in range(workers)]
                    for mode in range(4):
                        for variant in (0, 1):
                            if mode < 2:
                                # No affinity exercises an empty ingress shard;
                                # oversized affinity checks wrapped placement.
                                tail = [variant, variant, variant]
                                if variant:
                                    tail += [16]
                            elif mode == 2:
                                tail = [variant]
                            else:
                                tail = [3, 63 if variant else 0]
                            seed("wake_protocol",
                                 f"protocol-{workers}-{shards}-{batch}-{clustered}-{mode}-{variant}",
                                 [mode] + cfg + tail)


if __name__ == "__main__":
    exceptions()
    ranges()
    graph_scopes()
    joins()
    saturation()
    scope_races()
    wake_protocols()
