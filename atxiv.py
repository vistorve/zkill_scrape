import requests
import json
import sys
import os
import math
import boto3
from boto3.dynamodb.conditions import Key, Attr

from collections import defaultdict
from datetime import datetime

GET, POST, PUT, PATCH, DELETE = (requests.get, requests.post, requests.put, requests.patch, requests.delete)

class AllianceTournamentIntel(object):

    def __init__(self):
        self.ship_cache = {}

        self.dynamo = boto3.resource('dynamodb')
        
    def http_call(self, method, path, data=None):
        """
        Call api at path with data.
        """
        req = method("{}/{}/".format("http://www.zkillboard.com/api", path), json=data)

        if req.status_code != 200:
            print req.json()
            raise Exception("{}".format(req.json()))
        return req

    def _call(self, method, path):
        print "calling {}".format(path)
        return method(path).json()

    def get_tournament_url(self, name):
        """
        Lookup tournament ID based on name
        """
        data = self._call(GET, "https://crest-tq.eveonline.com/tournaments/")
        for item in data['items']:
            if name in item['href']['name']:
                print "Found matching tournament: {}".format(item['href']['name'])
                return item['href']['href']

    def _process_team_data(self, data_url, tournament):
        data = self._call(GET, data_url)
        for team in data['entries']:
            name = team['name']
            team_stats = team['teamStats']['href']
            self.dynamo.Table('teams').put_item(
                                 Item={'name': name,
                                       'tournament': tournament,
                                       'team_stats': team_stats})
        next_url = data.get('next', {'href': None})['href']
        return next_url

    def build_team_cache(self, tournament):
        tournament_url = self.get_tournament_url(tournament)
        if tournament_url is None:
            raise ValueError("Could not find a tournament containing '{}'".format(tournament))
        
        next_url = self._process_team_data(tournament_url, tournament_url)
        while next_url is not None:
            next_url = self._process_team_data(next_url, tournament_url)

    def matches_for_team(self, team_name, tournament):
        tournament_url = self.get_tournament_url(tournament)
        if tournament_url is None:
            raise ValueError("Could not find a tournament containing '{}'".format(tournament))

        team, team_stats = (None, None)
        for item in self.dynamo.Table('teams').scan()['Items']:
            if item['tournament'] != tournament_url: continue
            if item['name'].lower().startswith(team_name.lower()):
                team = item['name']
                team_stats = item['team_stats']
                break

        if team is None:
            raise ValueError("Could not find a team starting with {}".format(team_name))
        else:
            print "Found team {} for input {}".format(team, team_name)

        data = self._call(GET, team_stats)
        if 'exception' in data:
            raise ValueError("Team {} did not play in tournament {}".format(team, tournament))

        return team, [x['href'] for x in data['matches']], tournament_url

    def get_ship_info(self, ship_url):
        # If not cached get value and cache it
            
        if ship_url not in self.ship_cache:
            resp = self.dynamo.Table('ships').query(
                                 KeyConditionExpression=Key('url').eq(ship_url))
            items = list(resp['Items'])
            if len(items) == 0:            
                volume = self._call(GET, ship_url)['volume']
                self.dynamo.Table('ships').put_item(Item={'url': ship_url,
                                                          'volume': int(volume)})
                self.ship_cache[ship_url] = volume
            else:
                volume = items[0]['volume']
                self.ship_cache[ship_url] = volume

        return self.ship_cache[ship_url]

    def get_ships_for_match(self, match_url, team_lookup):
        match_data = self._call(GET, match_url)
        data = self._call(GET, "{}pilotstats/".format(match_url))

        ships = defaultdict(list)
        for item in data['items']:
            ship = item['shipType']['name']
            ship_volume = self.get_ship_info(item['shipType']['href'])
            pilot = item['pilot']['name']
            team = item['team']['href']
            if team not in team_lookup:
                team_lookup[team] = self._call(GET, team)['name']
            team = team_lookup[team]
            destroyed = item['isDead']
            damage_received = item.get('damageReceived', None)
            killer = item['killer']['name'] if destroyed else None

            ships[team].append(({'name': pilot, 'ship': ship, 'dmg': damage_received, 'killer': killer}, destroyed, ship_volume))

        if 'next' in data:
            for team, ship_list in get_ships_for_match(data['next']['href'], team_lookup):
                ships[team] += ship_list

        return ships, match_data

    def download_zk_data(self, pull_date):
        prev_kill_count = -1
        kill_count = 0
        i = 0
        zk_table = self.dynamo.Table('zkill')
        last_zk = list(self.dynamo.Table('last_zkill').scan()['Items'])[0]['time']
        if pull_date is None:
            last_zk = datetime.strptime(last_zk, '%Y-%m-%d %H:%M:%S')
        else:
            last_zk = datetime.strptime(pull_date, '%Y-%m-%d')
        # print last_zk
        written_killtime = False
        while prev_kill_count != kill_count:
            data = self.http_call(GET, "regionID/10000004/page/{}".format(i)).json()
            i += 1
            prev_kill_count = kill_count
            kill_count += len(data)

            # if not written_killtime:
            #     self.dynamo.Table('last_zkill').put_item(
            #         Item={'key': 'a',
            #               'time': data[0]['killTime']})
            #     written_killtime = True
            
            for kdata in data:
                killTime = datetime.strptime(kdata['killTime'], '%Y-%m-%d %H:%M:%S')
                # print killTime
                if killTime < last_zk: break
                zk_table.put_item(
                    Item={'zkillid': str(kdata['killID']),
                          'victim': kdata['victim']['characterName'],
                          'dmg': kdata['victim']['damageTaken'],
                          'killer': [x['characterName'] for x in kdata['attackers'] if x['finalBlow'] == 1][0]})
            try:
                killTime = datetime.strptime(kdata['killTime'], '%Y-%m-%d %H:%M:%S')
            except:
                print kdata
                raise
            if killTime < last_zk: break

            print i, killTime, kill_count

    def match_with_zk(self, ship):
        resp = self.dynamo.Table('zkill').query(KeyConditionExpression=Key('victim').eq(ship['name']))
        for kill in resp['Items']:
            #if 'Destoya' == ship['name']:
            #    print "{}:{} {}:{}".format(ship['dmg'], kill['dmg'], ship['killer'], kill['killer'])
            if kill['killer'] != ship['killer']: continue

            return kill['zkillid']

    def print_matches_for_team(self, team_name, tournament, raw=False, force=False, pull_date=None):
        # Get all matches for a team
        team, matches, tourn_url = self.matches_for_team(team_name, tournament)

        if not force:
            resp = list(self.dynamo.Table('rendered_matches').query(KeyConditionExpression=Key('team').eq(team) & Key('tournament_url').eq(tourn_url))['Items'])

            if len(resp) != 0:
                if raw:
                    return '<html><textarea style="font-family:Courier New">{}</textarea></html>'.format(resp[0]['raw'])
                return resp[0]['html']
        else:
            # Update zk
            self.download_zk_data(pull_date)

        html = "<html><body>"
        raw_out = []
        team_lookup = {}
        # For each match
        for match in matches:
            # Get the ships for both teams
            ships, match_data = self.get_ships_for_match(match, team_lookup)

            color_map = {'blueTeam': match_data['blueTeam']['teamName'],
                         'redTeam': match_data['redTeam']['teamName']}
            score = {color_map['blueTeam']: match_data['score']['blueTeam'],
                     color_map['redTeam']: match_data['score']['redTeam']}
            bans = {color_map['blueTeam']: [x['name'] for x in match_data['bans']['blueTeam'][0]['typeBans']],
                    color_map['redTeam']: [x['name'] for x in match_data['bans']['redTeam'][0]['typeBans']]}
            
            # Output
            team_sort = [x[0] for x in sorted([(x, 0 if x == team else 1) for x in  ships.keys()], key=lambda x: x[1])]
            
            html += self.make_html_output(team_sort, ships, score, bans, match)
            raw_out.append(self.make_raw_output(team_sort, ships, score, bans, match))
        html += "</body></html>"
        raw_output = "\n\n".join(raw_out)
        if raw_output == "":
            raw_output = "No matches found for team {}".format(team)
        self.dynamo.Table('rendered_matches').put_item(
            Item={'team': team,
                  'tournament_url': tourn_url,
                  'html': html,
                  'raw': raw_output})
        if raw:
            return '<html><textarea style="font-family:Courier New">{}</textarea></html>'.format(raw_output)
        return html

    def make_html_output(self, team_sort, ships, score, bans, match_url):
        html = '{}<br>'.format(match_url)
        html += '<table><tr><th>{} ({})</th><th>{} ({})</th></tr>'.format(
            team_sort[0],
            score[team_sort[0]],
            team_sort[1],
            score[team_sort[1]])
        n_bans = len(bans[team_sort[0]])
        for i in xrange(n_bans):
            html += '<tr>'
            for team in team_sort:
                html += '<td>{}</td>'.format(bans[team][i])
            html += '</tr>'
        html += '</table><br>'
        html += '<table style="width:100%"><tr><th>Pilot</th><th>Ship</th><th>Zkillboard Link</th></tr>'
        for name in team_sort:
            for ship in sorted(ships[name], key=lambda x: x[-1], reverse=True):
                ship_info, destroyed, volume = ship
                if destroyed:
                    zk_match = self.match_with_zk(ship[0])
                    if zk_match is None:
                        if ship_info['killer'].startswith('CCP'):
                            zk_match = "Boundary Violation (Killed by {})".format(ship_info['killer'])
                        else:
                            zk_match = "can't find matching zkillboard entry"
                    else:
                        zk_match = '<a href="https://zkillboard.com/kill/{}">zkill link</a>'.format(zk_match)
                else:
                    zk_match = ''
                html += "<tr><td>"
                html += "</td><td>".join((ship_info['name'], ship_info['ship'], zk_match))
                html += "</td></tr>"
            html += "<tr><td>---</td><td>---</td><td>---</td></tr>"

        html += "</table><br>"
        return html

    def make_raw_output(self, team_sort, ships, score, bans, match_url):
        lines = [match_url]
        ban_length = max(len(b) for b in bans[team_sort[0]])
        team_1 = "{} ({})".format(team_sort[0], score[team_sort[0]])
        team_2 = "{} ({})".format(team_sort[1], score[team_sort[1]])
        diff = ban_length - len(team_1)
        if diff > 0:
            team_1 += " " * diff
        lines.append("{} vs {}".format(team_1, team_2))
        for i in xrange(len(bans.values()[0])):
            line = ""
            line += bans[team_sort[0]][i]
            line += " " * (len(team_1) + len(" vs ") - len(line))
            line += bans[team_sort[1]][i]
            lines.append(line)
        lines.append("")
        col_1_length = max(len(s[0]['name']) for s in ships.values()[0] + ships.values()[1])
        col_2_length = max(len(s[0]['ship']) for s in ships.values()[0] + ships.values()[1])
        for name in team_sort:
            for ship in sorted(ships[name], key=lambda x: x[-1], reverse=True):
                ship_info, destroyed, volume = ship
                if destroyed:
                    zk_match = self.match_with_zk(ship_info)
                    if zk_match is None:
                        if ship_info['killer'].startswith('CCP'):
                            zk_match = "Boundary Violation (Killed by {})".format(ship_info['killer'])
                        else:
                            zk_match = "can't find matching zkillboard entry"
                    else:
                        zk_match = 'https://zkillboard.com/kill/{}'.format(zk_match)
                else:
                    zk_match = ''

                line = ship_info['name'] + " " * (4 + col_1_length - len(ship_info['name']))
                line += ship_info['ship'] + " " * (4 + col_2_length - len(ship_info['ship']))
                line += zk_match
                lines.append(line)
            lines.append("-"*10)
        return "\n".join(lines)


def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        'body': err.message if err else res,
        'headers': {
            'Content-Type': 'text/html',
        },
    }

def team_intel_handler(event, context):
    params = event['queryStringParameters']
    intel_tool = AllianceTournamentIntel()
    try:
        html = intel_tool.print_matches_for_team(params['team'], params['tournament'], params.get('raw', False), params.get('force', False), params.get('pull_date', None))
    except Exception, e:
        return respond(e)
    return respond(None, res=html)

def team_pull_handler(event, context):
    params = event['queryStringParameters']
    intel_tool = AllianceTournamentIntel()
    intel_tool.build_team_cache(params['tournament'])

if __name__ == '__main__':
    at, team = sys.argv[1:]
    intel_tool = AllianceTournamentIntel()
    #intel_tool.download_zk_data(2016)
    #intel_tool.build_team_cache(at)
    html = intel_tool.print_matches_for_team(team, at, True, True, '2017-01-01')
    print html
    #open("out.html", "w").write(html)
