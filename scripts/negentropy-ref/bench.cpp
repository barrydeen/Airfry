// Baseline benchmark for the reference negentropy C++ implementation.
//
// This is the exact library that strfry embeds (strfry vendors hoytech/negentropy and calls
// `Negentropy<StorageBase>::reconcile`). Measuring it here gives a faithful strfry baseline for
// the same reconciliation workload airfry-negentropy is benchmarked on.
//
// Build:
//   g++ -std=c++20 -O3 -I../../cpp -I./hoytech-cpp -I../../cpp/vendor/lmdbxx/include \
//       bench.cpp -lcrypto -o bench
//
// Usage: ./bench <n_items> <overlap_pct> <rounds>
//   overlap_pct: 0 = disjoint, 50 = half shared, 99 = 99% shared.
//
// Output (one line, machine parseable):
//   impl=strfry-ref n=<n> overlap=<pct> best_ms=<ms> rounds=<r> bytes=<b>

#include <negentropy.h>
#include <negentropy/storage/Vector.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include <openssl/sha.h>

static std::string makeId(uint64_t seed) {
    unsigned char d[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char *>(&seed), sizeof(seed), d);
    return std::string(reinterpret_cast<char *>(d), 32);
}

// Build a sealed Vector storage with `n` items. `offset` shifts the seed space so two sets can
// be made disjoint or partially overlapping.
static void buildSet(negentropy::storage::Vector &v, size_t n, uint64_t offset) {
    for (size_t i = 0; i < n; i++) {
        uint64_t seed = offset + i;
        v.insert(1700000000ULL + seed, makeId(seed));
    }
    v.seal();
}

// Drive a full initiator<->responder reconciliation. Returns bytes exchanged and rounds.
static void reconcile(negentropy::storage::Vector &client, negentropy::storage::Vector &server,
                      size_t &rounds, size_t &bytes) {
    negentropy::Negentropy<negentropy::storage::Vector> init(client, 0);
    negentropy::Negentropy<negentropy::storage::Vector> resp(server, 0);

    std::string msg = init.initiate();
    bytes += msg.size();
    rounds = 0;

    while (true) {
        rounds++;
        std::string reply = resp.reconcile(msg);
        bytes += reply.size();
        if (reply.size() == 1) break;

        std::vector<std::string> have, need;
        auto next = init.reconcile(reply, have, need);
        if (!next.has_value()) break; // initiator is done (version-only reply)
        bytes += next->size();
        msg = *next;
        if (rounds > 100000) {
            fprintf(stderr, "reconciliation did not terminate\n");
            exit(1);
        }
    }
}

int main(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "usage: %s <n_items> <overlap_pct> <rounds>\n", argv[0]);
        return 2;
    }
    size_t n = strtoull(argv[1], nullptr, 10);
    int overlap = atoi(argv[2]);
    int rounds = atoi(argv[3]);

    // Client set: seeds [0, n).
    // Server set: `shared` seeds taken from the client's range, the rest from a disjoint range.
    size_t shared = (n * (size_t)overlap) / 100;

    negentropy::storage::Vector client;
    buildSet(client, n, 0);

    negentropy::storage::Vector server;
    // Shared items (same seeds as client) plus disjoint items.
    for (size_t i = 0; i < shared; i++) {
        uint64_t seed = i;
        server.insert(1700000000ULL + seed, makeId(seed));
    }
    for (size_t i = shared; i < n; i++) {
        uint64_t seed = n + i;
        server.insert(1700000000ULL + seed, makeId(seed));
    }
    server.seal();

    double best_ms = 1e18;
    size_t best_rounds = 0, best_bytes = 0;
    for (int r = 0; r < rounds; r++) {
        auto t0 = std::chrono::steady_clock::now();
        size_t rr = 0, bb = 0;
        reconcile(client, server, rr, bb);
        auto t1 = std::chrono::steady_clock::now();
        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        if (ms < best_ms) {
            best_ms = ms;
            best_rounds = rr;
            best_bytes = bb;
        }
    }

    printf("impl=strfry-ref n=%zu overlap=%d best_ms=%.4f rounds=%zu bytes=%zu\n",
           n, overlap, best_ms, best_rounds, best_bytes);
    return 0;
}
