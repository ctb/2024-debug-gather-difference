#! /usr/bin/env python
"""
Compare two gather outputs and complain if they are different.

Currently handles:
* 
"""
import sys
import csv
import argparse
import pandas as pd
from itertools import zip_longest

import sourmash


def read_and_regularize_gather(csvfile):
    print(f"loading '{csvfile}'")
    df = pd.read_csv(csvfile)

    if 'f_unique_to_query' not in df.columns:
        raise ValueError(f"'{csvfile}' does not have 'f_unique_to_query_column' so this is probably not a gather output CSV")

    if ('name' in df.columns and
        'md5' in df.columns and
        'filename' in df.columns):

        print(f"renaming old-school gather column names to Tessa's favorite names")
        df.rename(columns={ 'name': 'match_name', 'md5': 'match_md5',
                            'filename': 'match_filename'}, inplace=True)
        assert 'name' not in df.columns

    return df


def extract_columns(df,
                    cols=('f_unique_to_query', 'match_name', 'remaining_bp')):
    gather_rows = [ list(tup[1]) for tup in df[list(cols)].iterrows() ]
    gather_rows.sort(reverse=True)

    return gather_rows


def main():
    p = argparse.ArgumentParser()
    p.add_argument('gather1_csv')
    p.add_argument('gather2_csv')
    p.add_argument('--never-fail', help="don't error exit")
    p.add_argument('--do-not-fix', help="don't fix columns (but do round)",
                   action='store_true')
    p.add_argument('--verbose', action='store_true',
                   help='be overly verbose')
    args = p.parse_args()

    df1 = read_and_regularize_gather(args.gather1_csv)
    df2 = read_and_regularize_gather(args.gather2_csv)

    print("loaded! examining column headers...")

    diffcol1 = set(df1.columns) - set(df2.columns)
    diffcol2 = set(df2.columns) - set(df1.columns)
    samecol = set(df1.columns).intersection(set(df2.columns))

    if diffcol1:
        print(f"in first, but not second: {diffcol1}")

    if diffcol2:
        print(f"in second, but not first: {diffcol2}")

    if 'potential_false_negative' in diffcol1:
        print(f"looks like '{args.gather1_csv}' is from sourmash gather v4!")
        print(f"(this is an acceptable difference. continuing!")
        diffcol1.remove('potential_false_negative')

    if 'potential_false_negative' in diffcol2:
        print(f"looks like '{args.gather2_csv}' is from sourmash gather v4!")
        print(f"(this is an acceptable difference. continuing!")
        diffcol2.remove('potential_false_negative')

    if diffcol1 or diffcol2:
        print(f"different columns remain: {diffcol1} {diffcol2}")
        if not args.never_fail:
            print(f"cannot resolve - exiting.")
            sys.exit(-1)

    core_rows1 = extract_columns(df1)
    core_rows2 = extract_columns(df2)

    print("\nexamining core column values...")
    fail = False
    for (f_unique1, name1, rbp1), (f_unique2, name2, rbp2) in \
        zip_longest(core_rows1, core_rows1, fillvalue=(None, None, None)):
        if name1 != name2:
            print("mismatch in names:", name, name2)
            fail = True
        if f_unique1 != f_unique2:
            print("mismatch in f_unique_to_query:", f_unique1, f_unique2)
            fail = True
        if rbp1 != rbp2:
            print("mismatch in remaining_bp:", rbp1, rbp2)
            fail = True

    if fail:
        if not args.never_fail:
            print(f"differences exist! exiting.")
            sys.exit(-1)
    else:
        print(f"congrats! two spreadsheets contain identical core columns!")

    round5 = lambda x: round(x, 5)
    fix_columns = {}
    fix_columns['std_abund'] = round5
    fix_columns['f_unique_weighted'] = round5

    if not args.do_not_fix:
        fix_columns['query_md5'] = lambda x: x[:8]

    xx_same = []
    xx_diff = []

    diff_values = {}
    for name in samecol:
        col1 = extract_columns(df1, cols=[name])
        col2 = extract_columns(df2, cols=[name])
        if name in fix_columns:
            if args.verbose:
                print(f"fixing column: {name}")
            fn = fix_columns[name]
            col1 = [ fn(i) for (i,) in col1 ]
            col2 = [ fn(i) for (i,) in col2 ]
        else:
            col1 = [ i for (i,) in col1 ]
            col2 = [ i for (i,) in col2 ]

        col1.sort()
        col2.sort()
        same = col1 == col2
        if not same:
            if name == 'f_match':
                import pprint
                for n, (x, y) in enumerate(zip_longest(col1, col2)):
                    pprint.pprint((n, x, y, x==y))
            xx_diff.append(name)
            diff_values[name] = (col1, col2)
        else:
            xx_same.append(name)

    if args.verbose:
        print("same:")
        print("*", "\n* ".join(xx_same))

    print("\n")

    if xx_diff:
        print("columns with differences:")
        print("*", "\n* ".join(xx_diff))
    else:
        print("** no columns with significant differences! :tada: **")

    if xx_diff:
        # remove still-expected differences.
        if 'match_filename' in xx_diff:
            xx_diff.remove('match_filename')
        if 'sum_weighted_found' in xx_diff:
            xx_diff.remove('sum_weighted_found')

        if xx_diff:
            return -1
    

if __name__ == '__main__':
    sys.exit(main())
