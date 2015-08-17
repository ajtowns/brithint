#!/usr/bin/env python

from contextlib import contextmanager
import argparse
import datetime
import operator
import re
import sqlalchemy
import sqlalchemy.ext.declarative
from sqlalchemy.sql import select, func
import sys
import os

class Tuples(object):
    def __init__(self, keys, rows):
        self.keys = keys
        self.rows = rows

    def __iter__(self):
        for r in self.rows:
            yield dict(zip(self.keys, r))

def merge_gens(*gens):
    gens = [iter(g) for g in gens]
    active = [True]*len(gens)
    first = [None]*len(gens)
    def upd(i):
        assert active[i]
        try:
            first[i] = gens[i].next()
        except StopIteration:
            active[i] = False

    for i,_ in enumerate(gens):
        upd(i)

    while True:
        bst, bst_i = None, None
        for i, f in enumerate(first):
            if active[i] and (bst is None or f < bst):
                bst, bst_i = f, i
        if bst_i is None:
            break
        yield bst
        upd(bst_i)

def queryresult(result):
    keys = tuple(result.keys())
    rows = [tuple(row) for row in result.fetchall()]
    return Tuples(keys=keys, rows=rows)

def query_to_dict(query, by):
    assert by in query.keys
    d = {}
    for rd in query:
        k = rd[by]
        del rd[by]
        d[k] = rd
    return d

def query_to_dict_tup(query, by):
    assert all(b in query.keys for b in by)
    d = {}
    for rd in query:
        k = tuple( rd[b] for b in by )
        for b in by:
            del rd[b]
        d[k] = rd
    return d

def print_query(query, *fields):
    if not fields:
        fields = query.keys
    fields = list(fields)

    fidx = []
    for f in fields:
        if ":" in f: f,_ = f.split(":")
        if f not in query.keys:
            raise Exception("Field %s not in query result (%s)" % 
                    (f,", ".join(query.keys)))
        fidx.append(query.keys.index(f))

    if query.rows:
        r = query.rows[0]
        widths = [max(5, len(str(r[fidx[i]]))) for i in range(len(fields))]
    else:
        widths = [5]*len(fields)
    for i, f in enumerate(fields):
        if ":" in f:
            f, w = f.split(":")
            fields[i] = f
            widths[i] = int(w)
        else:
            widths[i] = max(widths[i], len(f))

    fmt = " ".join("%%-%ds" % (w,) for w in widths)
    dash = " ".join("="*w for w in widths)

    print fmt % tuple(fields)
    print dash
    for r in sorted(query.rows, key=lambda r: r[fidx[0]]):
        print fmt % tuple(r[fidx[i]] for i in range(len(fields)))


def print_history(hist_info, hist_msg):
    def fmt_hist(tbl_msg, tbl_hist):
        for d in tbl_hist:
            m = tbl_msg[d["cdm"]] % (d)
            if d["user"] is not None:
                m += " by %s" % (d["user"],)
            if d["reason"] is not None:
                m += " (reason: %s)" % (d["reason"],)
            ext = {}
            if d["cdm"] == "m":
                for k in d.keys():
                    o = "orig_"+k
                    if o in d.keys() and d[k] != d[o]:
                        ext[k] = (d[o], d[k])
            yield (d["event_id"], d["timestamp"], m, ext)

    gens = [fmt_hist(hist_msg[t], hist_info[t]) for t in hist_info]
    for ev, ts, m, ext in merge_gens(*gens):
        print "%s %s" % (ts.strftime("%Y-%m-%d %H:%M:%S"), m)
        for k in sorted(ext):
            print "%24s%s: %s -> %s" % ("", k, ext[k][0], ext[k][1])

