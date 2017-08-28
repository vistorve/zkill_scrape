import requests
import json
import sys

from collections import defaultdict
from datetime import datetime

GET, POST, PUT, PATCH, DELETE = (requests.get, requests.post, requests.put, requests.patch, requests.delete)

def http_call(method, path, data=None):
    """
    Call api at path with data.
    """
    req = method("{}/{}/".format("http://www.zkillboard.com/api", path), json=data)

    if req.status_code != 200:
        print req.json()
        raise Exception("{}".format(req.json()))
    return req

def check_alliance(alliance):
    resp = http_call(GET, "stats/allianceID/{}".format(alliance))
    data = resp.json()

    # Base checks
    try:
        active_chars = data['activepvp']['characters']['count']
        if active_chars < 50:
            return None, "not enough active chars {}".format(active_chars)
        if data['activepvp']['corporations']['count'] > 10:
            return None, "too many active corps {}".format(data['activepvp']['corporations']['count'])
        if data['info']['memberCount'] > 3000:
            return None, "too many members {}".format(data['info']['memberCount'])
    except:
        print alliance, data

    # Activity
    # ratios = []
    # for yearmonth, stats in data['months'].iteritems():
    #     if not yearmonth.startswith('2017'): continue
    #     ratio = (stats['shipsLost'] + stats['shipsDestroyed'])/float(active_chars)
    #     if ratio > 30 or ratio < 10:
    #         return None, "bad ratio {}".format(ratio)
    #     ratios.append((yearmonth, ratio))

    active_ratio = data['info']['memberCount']/float(active_chars)
    if active_ratio > 8:
        return None, "ratio of total to active too high {}".format(active_ratio)
    return data['info']['name']#, ratios


def download(path, save_file):
    killTime = datetime(2017,4,22)
    endtime = datetime(2016,2,1)
    prev_kill_count = -1
    killcount = 0
    i = 0
    f = open(save_file,"w")
    while killTime > endtime and prev_kill_count != killcount:
        data = http_call(GET, "{}/{}".format(path, i)).json()
        i += 1
        prev_kill_count = killcount
        killcount += len(data)
        for kdata in data:
            f.write(json.dumps(kdata) + "\n")
        try:
            killTime = datetime.strptime(kdata['killTime'], '%Y-%m-%d %H:%M:%S')
        except:
            print kdata
            raise
        print i, killTime, killcount
    f.close()

def get_top_alliances(kdata_file):
    attackers = defaultdict(int)
    victims = defaultdict(int)
    involved = defaultdict(int)
    i = 0
    killcount = 0
    with open(kdata_file, 'r') as f:
        for l in f:
            kdata = json.loads(l)
            victim_alliance = kdata['victim']['allianceID']
            final_blow = [a for a in kdata['attackers'] if a['finalBlow'] == 1][0]

            # Skip if victim in same alliance as final blower
            if victim_alliance == final_blow['allianceID']: continue
            # Skip if there were no items in wreck to avoid farm kills
            if len(kdata['items']) == 0: continue
            # Skip if npc kill
            if kdata['zkb']['npc']: continue
            # Skip if kill was a pod
            if kdata['victim']['shipTypeID'] == 670: continue
            # Skip if damage taken is absuredly low
            if kdata['victim']['damageTaken'] < 1000: continue


            attackers[(final_blow['allianceName'], final_blow['allianceID'])] += 1
            victims[(kdata['victim']['allianceName'], kdata['victim']['allianceID'])] += 1
            involved[(final_blow['allianceName'], final_blow['allianceID'])] += len(kdata['attackers'])
        killTime = datetime.strptime(kdata['killTime'], '%Y-%m-%d %H:%M:%S')
        print i, killTime, killcount
        i += 1
    return attackers, victims, involved

bastard_cartel = 99003894
agony = 1119479143
alliance_kms = "nullsec/api/allianceID/{}/year/{}/month/{}/page"

def check_alliance(alliance_id, kdata_file):
    n_attackers = 0
    n_kills = 0
    with open(kdata_file, 'r') as f:
        for l in f:
            kdata = json.loads(l)
            final_blow = [a for a in kdata['attackers'] if a['allianceID'] == alliance_id]
            if len(final_blow) == 0: continue
            #if final_blow['allianceID'] !=  alliance_id: continue

            n_kills += 1
            n_attackers += len(kdata['attackers'])
    print n_kills, n_attackers
    print n_attackers/float(n_kills)

def output_kill_hour(alliance_id, kdata_file):
    with open(kdata_file, 'r') as f:
        for l in f:
            kdata = json.loads(l)
            final_blow = [a for a in kdata['attackers'] if a['finalBlow'] == 1][0]
            if final_blow['allianceID'] !=  alliance_id: continue
            killTime = datetime.strptime(kdata['killTime'], '%Y-%m-%d %H:%M:%S')
            print killTime.hour



#download(alliance_kms.format(bastard_cartel, 2017, "02"), "cartel_kms_2017-02")
#sys.exit(0)

#check_alliance(bastard_cartel, "cartel_kms_2017-02")
#sys.exit(0)

output_kill_hour(agony, "agony_kms_2016-03")
sys.exit(0)

# attackers, victims, involved = get_top_alliances("kdata")
# with open('top_alliance_attackers','w') as f:
#     for (name, id), count in attackers.iteritems():
#         if id == 0: continue
#         f.write("{}-{}\t{}\n".format(name, id, count))
# with open('top_alliance_victims','w') as f:
#     for (name, id), count in victims.iteritems():
#         if id == 0: continue
#         f.write("{}-{}\t{}\n".format(name, id, count))
# with open('small_gang_alliances', 'w') as f:
#     for (name, id), count in involved.iteritems():
#         if id == 0: continue
#         avg_involved = count/float(attackers[(name, id)])
#         if avg_involved < 20 and avg_involved > 10:
#             print name, id, avg_involved
#             f.write("{}-{}\t{}\n".format(name, id, avg_involved))
# for (name, id), count in sorted(counts.items(), key=lambda x: x[1], reverse=True)[:100]:
#     print name, id, count


alliances = []
with open('top_alliance_attackers','r') as f:
    for l in f:
        name_id, count = l.strip().split("\t")
        name, id = name_id.rsplit("-", 1)
        if id == "0": continue
        alliances.append((name, id, int(count)))

small_gang = {}
with open('small_gang_alliances','r') as f:
    for l in f:
        name_id, count = l.strip().split("\t")
        name, id = name_id.rsplit("-", 1)
        if id == "0": continue
        small_gang[name] = count

for name, id, count in sorted(alliances, key=lambda x: x[2], reverse=True):
    if count < 50: break
    if name not in small_gang: continue

    resp = check_alliance(id)
    if resp[0] is None:
        print count, name, resp[1]
    else:
        print count, small_gang[name], resp, "-"*20
