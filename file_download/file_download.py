
import time
import boto
from boto.s3.key import Key

# Step 1. Download 5mb S3 file.


conn = boto.connect_s3(ACCESS_KEY, SECRET_KEY)
bucket = conn.get_bucket(bucketname)

bucket_list = bucket.list()
for l in bucket.list():
    #print(l)
    keyString = str(l.key)
    if keyString == "50mb.txt":
        t0 = time.time()
        l.get_contents_to_filename(keyString)
        t1 = time.time()

        d_time = (t1-t0)

        print(d_time)

# ('100mb.txt', 1.8089179992675781)
# ('1gb.txt', 16.896674871444702)
# ('1mb.txt', 0.06831717491149902)
# ('250mb.txt', 3.3532068729400635)
# ('25mb.txt', 0.3604848384857178)
# ('500mb.txt', 6.501723051071167)
# ('50mb.txt', 1.3446698188781738)
# ('5gb.txt', 60.357163190841675)
# ('5mb.txt', 0.15212798118591309)
