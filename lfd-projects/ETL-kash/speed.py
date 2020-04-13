import time
start = time.time()
for i in range(50000000): 
	i * i + i
end = time.time()
print(end - start)

# kasht server average of 2 attempts 15.01601767539978
# My laptop    average or 2 attempts 4.370018005371094

# kasht server processed 2070 files in 78 Minutes
# My laptop    processed 2070 files in 40 Minutes