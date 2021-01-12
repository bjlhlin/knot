#!/usr/bin/env python3

'''Test of transferring members between catalogs.'''

from dnstest.test import Test
from dnstest.utils import set_err, detail_log
import dnstest.params

import glob
import shutil
import subprocess
import random
import os

SLEEP = 3

def gen_zonefile(server, zone_name):
    filename = master.dir + "/master/" + zone_name + "zone"
    if os.path.exists(filename):
        os.remove(filename)
    with open(filename, "w") as zf:
        zf.write("$ORIGIN " + zone_name + "\n")
        zf.write("$TTL 3600\n")
        zf.write("@ SOA a a 1 10800 3600 1209600 7200\n")
        zf.write("@ NS a\na A 1.2.3.4\n")

def add_member(update, zone_name, uniq):
    update.add("junk." + uniq + ".zones", 0, "PTR", zone_name)

def rem_member(update, zone_name, uniq):
    update.delete("junk." + uniq + ".zones", "PTR", zone_name)

def dnskey_cmp_base(a, b, msg):
    shall_match = 0
    match = 0
    for x in a.resp.answer[0].to_rdataset():
        shall_match += 1
        for y in b.resp.answer[0].to_rdataset():
            if x.to_text() == y.to_text():
                match += 1
    if shall_match < 1:
        set_err("NO DNSKEY: " + msg)
    return shall_match, match

def dnskey_check_match(a, b, msg):
    shall, match = dnskey_cmp_base(a, b, msg)
    if match != shall:
        set_err("DNSKEY NOT MATCH: " + msg)

def dnskey_check_purge(a, b, msg):
    shall, match = dnskey_cmp_base(a, b, msg)
    if match != 0:
        set_err("DNSKEY NOT PURGE: " + msg)

t = Test(stress=False)

master = t.server("knot")

zone = t.zone("example.com.") + t.zone("example.")

t.link(zone, master)

for z in zone:
    master.zones[z.name].catalog = True
    master.dnssec(z).enable = True # this is only useful for signing the member zones

t.start()
t.sleep(SLEEP * 2)

# set up member1. inside zone[0]
up = master.update(zone[0])
up.add("version", 0, "TXT", "2")
add_member(up, "member1.", "m1u1")
gen_zonefile(master, "member1.")
up.send()
t.sleep(SLEEP)
resp1 = master.dig("member1.", "DNSKEY")
resp1.check(rcode="NOERROR")

# transfer member1. to zone[1] w/o purge
up = master.update(zone[1])
up.add("version", 0, "TXT", "2")
add_member(up, "member1.", "m1u1")
up.send()
t.sleep(SLEEP)
up = master.update(zone[0])
rem_member(up, "member1.", "m1u1")
up.send()
t.sleep(SLEEP)
resp2 = master.dig("member1.", "DNSKEY")
resp2.check(rcode="NOERROR")
dnskey_check_match(resp1, resp2, "transfer1")

# transfer member2. to zone[0] with purge and shadow
up = master.update(zone[0])
add_member(up, "member1.", "m1u2")
add_member(up, "member1.", "m1u3")
up.send()
t.sleep(SLEEP)
up = master.update(zone[1])
add_member(up, "member1.", "m1u4")
rem_member(up, "member1.", "m1u1")
add_member(up, "member1.", "m1u5")
up.send()
t.sleep(SLEEP)
gen_zonefile(master, "member1.")
master.ctl("zone-reload")
t.sleep(SLEEP)
resp3 = master.dig("member1.", "DNSKEY")
resp3.check(rcode="NOERROR")
dnskey_check_purge(resp2, resp3, "transfer2")

t.end()
