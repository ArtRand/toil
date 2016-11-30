# Copyright (C) 2015 UCSC Computational Genomics Lab
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging

from toil.test import needs_mesos, ApplianceTestSupport, needs_appliance

logger = logging.getLogger(__name__)


@needs_appliance
@needs_mesos
class ClusterStatsTest(ApplianceTestSupport):

    def testStats(self):
        def userScript():
            from toil.job import Job
            from toil.common import Toil
            import time

            # noinspection PyUnusedLocal
            def job(job, disk='10M', cores=1, memory='10M'):
                # make the job run for 2 minutes so we can gather some stats
                time.sleep(2 * 60)

            if __name__ == '__main__':
                options = Job.Runner.getDefaultArgumentParser().parse_args()
                with Toil(options) as toil:
                    toil.start(Job.wrapJobFn(job))

        def assertJSON():
            import os
            import json

            files = os.listdir('/data/')
            jsonFiles = [fileName for fileName in files if fileName.endswith('.json')]
            assert len(jsonFiles) == 1
            with open(jsonFiles.pop()) as f:
                jsonString = f.read()
            try:
                json.loads(jsonString)
            except ValueError:
                self.fail('Invalid JSON: %s is not valid JSON' % jsonString)

        with self._venvApplianceCluster() as (leader, worker):
            script = self._getScriptSource(userScript)
            leader.deployScript(script=script, path=self.sitePackages, packagePath='testuserScript')
            pythonArgs = ['venv/bin/python', 'testuserScript.py']
            toilArgs = ['--logDebug',
                        '--batchSystem=mesos',
                        '--mesosMaster=localhost:5050',
                        '--defaultMemory=10M',
                        '--clusterStats=/data/',
                        '/data/jobstore']
            command = pythonArgs + toilArgs
            leader.runOnAppliance(*command)
            jsonScript = self._getScriptSource(assertJSON)
            leader.deployScript(script=jsonScript, path=self.sitePackages,
                                packagePath='checkJson.py')
            jsonArgs = ['venv/bin/python', 'checkJson.py']
            leader.runOnAppliance(*jsonArgs)
