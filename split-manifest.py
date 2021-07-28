#! /usr/bin/env python
# CTB: add select stuff? or no? or maybe shuffle... :think:
import sys
import argparse
import sourmash


def main():
    p = argparse.ArgumentParser()
    p.add_argument('-n', '--num-batches', default=25, type=int,
                   help="number of files to split into")
    p.add_argument('-o', '--output-prefix', required=True,
                   help="path/name to prefix all output split files with")
    p.add_argument('db')
    args = p.parse_args()

    db = sourmash.load_file_as_index(args.db)
    assert db.manifest
    manifest = db.manifest

    n_rows = len(manifest.rows)
    batch_size = n_rows // args.num_batches

    print(f"loaded a manifest with {n_rows} signatures from '{args.db}'")
    print(f"now splitting into {args.num_batches} batches of approx {batch_size} signatures each")

    batches = []
    for n in range(0, args.num_batches):
        start = n * batch_size
        end = start + batch_size
        batches.append(list(manifest.rows[start:end]))

    if end < n_rows:
        for i, row in enumerate(manifest.rows[end:]):
            batches[i].append(row)

    assert len(batches) == args.num_batches

    for batch_n, batch in enumerate(batches):
        outname = f"{args.output_prefix}.{batch_n+1}.csv"

        batch_manifest = sourmash.manifest.CollectionManifest(batch)
        with open(outname, "w", newline='') as outfp:
            batch_manifest.write_to_csv(outfp, write_header=True)

    print(f"wrote {len(batches)} batched manifests with prefix {args.output_prefix}")


if __name__ == '__main__':
    sys.exit(main())
