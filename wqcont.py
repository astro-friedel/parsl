# test what happens when there are tasks outstanding at exit time

# this *should* cause parsl to shut down properly including the process exiting

import parsl
import time

# parsl.set_stream_logger()

@parsl.python_app
def longlongapp(t):
    import time
    time.sleep(t)


from parsl.tests.configs.htex_local_alternate import config
#from parsl.tests.configs.workqueue_ex import config
#from parsl.tests.configs.workqueue_monitoring import config

#parsl.load()
parsl.load(config)

futs = []
for n in range(0,200000):
    futs.append(longlongapp(3600))

print("all tasks submitted")
for f in futs:
    f.result()


