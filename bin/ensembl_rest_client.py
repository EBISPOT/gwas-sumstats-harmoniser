import requests
import time

class EnsemblRestClient(object):
    def __init__(self, server='http://rest.ensembl.org', reqs_per_sec=15):
        self.server = server
        self.reqs_per_sec = reqs_per_sec
        self.req_count = 0
        self.last_req = 0

    def perform_rest_action(self, endpoint, hdrs=None):
        hdrs = self.set_json_format(hdrs)
        self.rate_check()
        data = None

        try:
            content = requests.get(self.server + endpoint, headers=hdrs)
            if self.slow_down_if_needed(content.headers):
                content = requests.get(self.server + endpoint, headers=hdrs)
            if content:
                data = content.json()
            else:
                data = 'NA'
            self.req_count += 1

        except requests.exceptions.HTTPError as ehe:
            # check if we are being rate limited by the server
            if self.slow_down_if_needed(ehe.headers):
                self.perform_rest_action(endpoint, hdrs)
        except requests.exceptions.RequestException as e:
                print('Request failed for {0}: Status: {1}\n'.format(
                endpoint, e))

        return data

    @staticmethod
    def slow_down_if_needed(headers):
        if 'Retry-After' in headers:
            retry = headers['Retry-After']
            time.sleep(float(retry)) # pragma: no cover
            return True

    @staticmethod
    def set_json_format(headers):
        if headers is None:
            headers = {}
        if 'Content-Type' not in headers:
            headers['Content-Type'] = 'application/json'
        return headers

    def rate_check(self):
        # check if we need to rate limit ourselves
        if self.req_count >= self.reqs_per_sec:
            time.sleep(1)
            delta = time.time() - self.last_req
            if delta < 1:
                time.sleep(1 - delta)
            self.last_req = time.time()
            self.req_count = 0

    def map_bp_to_build_with_rest(self, chromosome, bp, build_orig, build_mapped):
        map_build = self.perform_rest_action(
            '/map/human/{orig}/{chromosome}:{bp}:{bp}/{mapped}?'.format(
            chromosome=chromosome, bp=bp, orig=build_orig, mapped=build_mapped)
            )
        return self.retrieve_mapped_item(map_build)

    @staticmethod
    def retrieve_mapped_item(map_build):
        if "mappings" in map_build:
            mappings = map_build["mappings"]
            if len(mappings) > 0:
                if "mapped" in mappings and "start" in mappings["mapped"]:
                    return map_build["mappings"][0]["mapped"]["start"]
        return 'NA'

    def resolve_rsid(self, chromosome, bp):
        rsid_request = self.perform_rest_action(
            '/overlap/region/human/{chromosome}:{bp}:{bp}?feature=variation'.format(
            chromosome=chromosome, bp=bp)
            )
        return self.retrieve_rsid(rsid_request)

    @staticmethod
    def retrieve_rsid(rsid_request):
        if len(rsid_request) > 0 and "id" in rsid_request[0]:
            return rsid_request[0]["id"]
        return 'id:NA'

    def check_orientation_with_rest(self, rsid):
        variation_request = self.perform_rest_action(
            '/variation/human/{rsid}?'.format(
            rsid=rsid)
            )
        return self.retrieve_strand(variation_request)

    # RETRIEVE STRAND - I DON'T BELIEVE THIS RETRIEVES THE CORRECT INFORMATION
    @staticmethod
    def retrieve_strand(variation_request):
        if "mappings" in variation_request:
            mappings = variation_request["mappings"]
            if len(mappings) > 0:
                if "strand" in mappings[0]:
                    return mappings[0]["strand"]
        return False

    def resolve_location_with_rest(self, rsid):
        variation_request = self.perform_rest_action(
            '/variation/human/{rsid}?'.format(
            rsid=rsid)
            )
        return self.retrieve_location(variation_request)

    def get_rsid(self, rsid):
        variant_info = self.perform_rest_action('/variation/human/{}?'.format(rsid))
        return variant_info

    @staticmethod
    def retrieve_location(variation_request):
        if "mappings" in variation_request:
            mappings = variation_request["mappings"]
            if len(mappings) > 0:
                if "location" in mappings[0]:
                    return mappings[0]["location"]
        return False
