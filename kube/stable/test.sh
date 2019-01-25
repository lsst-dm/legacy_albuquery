#!/bin/bash -ex
curl --fail -o /tmp/db_result.json -L https://lsst-lsp-stable.ncsa.illinois.edu/api/db/v1/tap/sync/ -v -d 'QUERY=SELECT * FROM W13_sdss_v2.sdss_stripe82_01.RunDeepForcedSource WHERE qserv_areaspec_poly(9.3,-1.2,9.5,-1.2,9.3,-1.1)' --max-time 600 --connect-timeout 300
