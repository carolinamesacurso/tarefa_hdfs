
# Tarea HDFS

## Preparar material

```
carolinamesamartine@026P230B:~$ cp -r "/mnt/c/Users/carolina.mesamartine/Desktop/BD/xera_historico.py" .
carolinamesamartine@026P230B:~$ ls
D5_lonxa        code2       mongoej              obradoiro_redis              ragejemplos         xera_historico.py
E4_concellos    concellos   mongotarea           obradoirogithubissue         reservas_albergues  ximnasio
Escritorio      env_backup  notas                plantilla                    salesCodificacion
agenteArchivos  festival    notasPruebas         proyecto_pruebaagentenotas1  tempo
agentecontexto  garage      notasejemplo         pruebaProyectoNotas          tempo2
code            jokes       obradoiroCassanadra  ragTarea                     voluntariado

carolinamesamartine@026P230B:~$ scp xera_historico.py xuedua076@hadoop.cesga.es:
** WARNING: connection is not using a post-quantum key exchange algorithm.
** This session may be vulnerable to "store now, decrypt later" attacks.
** The server may need to be upgraded. See https://openssh.com/pq.html
xuedua076@hadoop.cesga.es's password:
xera_historico.py                                                                     100% 2097    68.8KB/s   00:00     


[xuedua076@cdh61-login1 ~]$ python xera_historico.py --mb 64 --saida reservas_historico.csv
reservas_historico.csv: 64.0 MB, 1659576 reservas
```

## Paso 1

```
[xuedua076@cdh61-login1 ~]$ hdfs getconf -confKey dfs.blocksize
134217728
```

```
[xuedua076@cdh61-login1 ~]$ hdfs getconf -confKey dfs.replication
3
```

```
[xuedua076@cdh61-login1 ~]$ hdfs getconf -confKey dfs.bytes-per-checksum
512
```

```
hdfs dfs -ls
```

```
[xuedua076@cdh61-login1 ~]$ hdfs dfs -df -h
Filesystem            Size     Used  Available  Use%
hdfs://nameservice1  2.5 P  283.2 T      2.2 P   11%
```
```
[xuedua076@cdh61-login1 ~]$ ls
reservas_historico.csv  xera_historico.py
```

```
[xuedua076@cdh61-login1 ~]$ head reservas_historico.csv
reserva_id,noite,albergue_id,peregrino_id,camino,prezo
1,2025-12-16,174,718235,Portugués,8
2,2026-12-18,20,61910,Norte,0
3,2021-11-18,35,172789,Fisterra,12
4,2021-10-15,269,175864,Inglés,0
5,2021-06-10,65,537529,Francés,12
6,2021-02-14,162,565255,Fisterra,8
7,2025-11-08,90,871236,Primitivo,8
8,2021-03-01,186,857590,Norte,18
9,2026-01-31,199,701231,Norte,15
```

```
[xuedua076@cdh61-login1 ~]$ hdfs dfs -mkdir -p e5
[xuedua076@cdh61-login1 ~]$ hdfs dfs -D dfs.blocksize=16m -put reservas_historico.csv e5/historico_16m.csv
[xuedua076@cdh61-login1 ~]$ hdfs dfs -put reservas_historico.csv e5/historico_def.csv
[xuedua076@cdh61-login1 ~]$ hdfs dfs -stat "%n · %b bytes · bloque %o · replicación %r" e5/*
historico_16m.csv ?? 67108925 bytes ?? bloque 16777216 ?? replicaci??n 3
historico_def.csv ?? 67108925 bytes ?? bloque 134217728 ?? replicaci??n 3
```

```
[xuedua076@cdh61-login1 ~]$ hdfs fsck /user/xuedua076/e5/historico_16m.csv -files -blocks -locations
Connecting to namenode via https://c14-18.bd.cluster.cesga.es:9871/fsck?ugi=xuedua076&files=1&blocks=1&locations=1&path=%2Fuser%2Fxuedua076%2Fe5%2Fhistorico_16m.csv
FSCK started by xuedua076 (auth:SIMPLE) from /10.121.242.31 for path /user/xuedua076/e5/historico_16m.csv at Wed Nov 18 21:16:48 CET 2026
/user/xuedua076/e5/historico_16m.csv 67108925 bytes, replicated: replication=3, 5 block(s):  OK
0. BP-562833966-10.121.14.18-1548250294268:blk_1088363506_14657524 len=16777216 Live_repl=3  [DatanodeInfoWithStorage[10.121.14.3:9866,DS-cef69aab-1919-4780-9a18-66ae0fca7730,DISK], DatanodeInfoWithStorage[10.121.14.13:9866,DS-9c46ff26-5bfe-4c47-8912-87a3d979bfb8,DISK], DatanodeInfoWithStorage[10.121.14.14:9866,DS-e5090a8a-3522-4ac2-b391-e66d323bea91,DISK]]
1. BP-562833966-10.121.14.18-1548250294268:blk_1088363507_14657525 len=16777216 Live_repl=3  [DatanodeInfoWithStorage[10.121.14.9:9866,DS-f5f358b7-7b1a-434b-be5f-f9fdb711fae8,DISK], DatanodeInfoWithStorage[10.121.14.6:9866,DS-cab15550-e61d-4aa4-9797-ac433944c7f8,DISK], DatanodeInfoWithStorage[10.121.14.3:9866,DS-5df6dcc3-ad81-4dbf-8cff-9d0e572032cf,DISK]]
2. BP-562833966-10.121.14.18-1548250294268:blk_1088363508_14657526 len=16777216 Live_repl=3  [DatanodeInfoWithStorage[10.121.14.14:9866,DS-2a1194a9-6d24-45f4-9291-574c1cf2dc63,DISK], DatanodeInfoWithStorage[10.121.14.8:9866,DS-0ba77bbb-4ffe-4e00-b92b-096b12e3b27c,DISK], DatanodeInfoWithStorage[10.121.14.9:9866,DS-709bced4-8573-4a5a-a557-7b7e7c690472,DISK]]
3. BP-562833966-10.121.14.18-1548250294268:blk_1088363509_14657527 len=16777216 Live_repl=3  [DatanodeInfoWithStorage[10.121.14.2:9866,DS-94426bad-1a4f-487d-a766-a06072aae947,DISK], DatanodeInfoWithStorage[10.121.14.7:9866,DS-765ee11a-08d5-4778-9643-d5130b720a10,DISK], DatanodeInfoWithStorage[10.121.14.14:9866,DS-417863e5-9075-4762-bbaa-ea4e6545011a,DISK]]
4. BP-562833966-10.121.14.18-1548250294268:blk_1088363510_14657528 len=61 Live_repl=3  [DatanodeInfoWithStorage[10.121.14.13:9866,DS-bb5cb880-4dae-4a3d-92da-ead25fca54ef,DISK], DatanodeInfoWithStorage[10.121.14.2:9866,DS-cd16174a-627e-4efb-93c7-fa6373c5a67c,DISK], DatanodeInfoWithStorage[10.121.14.12:9866,DS-a79f886c-a581-4d27-840b-5c590707bd37,DISK]]


Status: HEALTHY
 Number of data-nodes:  14
 Number of racks:               1
 Total dirs:                    0
 Total symlinks:                0

Replicated Blocks:
 Total size:    67108925 B
 Total files:   1
 Total blocks (validated):      5 (avg. block size 13421785 B)
 Minimally replicated blocks:   5 (100.0 %)
 Over-replicated blocks:        0 (0.0 %)
 Under-replicated blocks:       0 (0.0 %)
 Mis-replicated blocks:         0 (0.0 %)
 Default replication factor:    3
 Average block replication:     3.0
 Missing blocks:                0
 Corrupt blocks:                0
 Missing replicas:              0 (0.0 %)

Erasure Coded Block Groups:
 Total size:    0 B
 Total files:   0
 Total block groups (validated):        0
 Minimally erasure-coded block groups:  0
 Over-erasure-coded block groups:       0
 Under-erasure-coded block groups:      0
 Unsatisfactory placement block groups: 0
 Average block group size:      0.0
 Missing block groups:          0
 Corrupt block groups:          0
 Missing internal blocks:       0
FSCK ended at Wed Nov 18 21:16:48 CET 2026 in 1 milliseconds


The filesystem under path '/user/xuedua076/e5/historico_16m.csv' is HEALTHY
```
```
[xuedua076@cdh61-login1 ~]$ hdfs fsck /user/xuedua076/e5/historico_def.csv -files -blocks -locations
Connecting to namenode via https://c14-18.bd.cluster.cesga.es:9871/fsck?ugi=xuedua076&files=1&blocks=1&locations=1&path=%2Fuser%2Fxuedua076%2Fe5%2Fhistorico_def.csv
FSCK started by xuedua076 (auth:SIMPLE) from /10.121.242.31 for path /user/xuedua076/e5/historico_def.csv at Wed Nov 18 21:17:57 CET 2026
/user/xuedua076/e5/historico_def.csv 67108925 bytes, replicated: replication=3, 1 block(s):  OK
0. BP-562833966-10.121.14.18-1548250294268:blk_1088363511_14657529 len=67108925 Live_repl=3  [DatanodeInfoWithStorage[10.121.14.13:9866,DS-39a05a8f-d0a6-4530-b25c-cfb1f97f1027,DISK], DatanodeInfoWithStorage[10.121.14.11:9866,DS-0bd94e71-7cca-4903-9f84-924e78b7b86c,DISK], DatanodeInfoWithStorage[10.121.14.10:9866,DS-17628fef-a754-4509-8161-c4e43285a545,DISK]]


Status: HEALTHY
 Number of data-nodes:  14
 Number of racks:               1
 Total dirs:                    0
 Total symlinks:                0

Replicated Blocks:
 Total size:    67108925 B
 Total files:   1
 Total blocks (validated):      1 (avg. block size 67108925 B)
 Minimally replicated blocks:   1 (100.0 %)
 Over-replicated blocks:        0 (0.0 %)
 Under-replicated blocks:       0 (0.0 %)
 Mis-replicated blocks:         0 (0.0 %)
 Default replication factor:    3
 Average block replication:     3.0
 Missing blocks:                0
 Corrupt blocks:                0
 Missing replicas:              0 (0.0 %)

Erasure Coded Block Groups:
 Total size:    0 B
 Total files:   0
 Total block groups (validated):        0
 Minimally erasure-coded block groups:  0
 Over-erasure-coded block groups:       0
 Under-erasure-coded block groups:      0
 Unsatisfactory placement block groups: 0
 Average block group size:      0.0
 Missing block groups:          0
 Corrupt block groups:          0
 Missing internal blocks:       0
FSCK ended at Wed Nov 18 21:17:57 CET 2026 in 1 milliseconds


The filesystem under path '/user/xuedua076/e5/historico_def.csv' is HEALTHY
```

```
[xuedua076@cdh61-login1 ~]$ hdfs dfs -du -h e5
64.0 M  192.0 M  e5/historico_16m.csv
64.0 M  192.0 M  e5/historico_def.csv
```
```
[xuedua076@cdh61-login1 ~]$ hdfs dfs -setrep 2 e5/historico_16m.csv
Replication 2 set: e5/historico_16m.csv
```

```
[xuedua076@cdh61-login1 ~]$ hdfs fsck /user/xuedua076/e5/historico_16m.csv -files -blocks -locations
Connecting to namenode via https://c14-18.bd.cluster.cesga.es:9871/fsck?ugi=xuedua076&files=1&blocks=1&locations=1&path=%2Fuser%2Fxuedua076%2Fe5%2Fhistorico_16m.csv
FSCK started by xuedua076 (auth:SIMPLE) from /10.121.242.31 for path /user/xuedua076/e5/historico_16m.csv at Wed Nov 18 21:21:52 CET 2026
/user/xuedua076/e5/historico_16m.csv 67108925 bytes, replicated: replication=2, 5 block(s):  OK
0. BP-562833966-10.121.14.18-1548250294268:blk_1088363506_14657524 len=16777216 Live_repl=2  [DatanodeInfoWithStorage[10.121.14.3:9866,DS-cef69aab-1919-4780-9a18-66ae0fca7730,DISK], DatanodeInfoWithStorage[10.121.14.14:9866,DS-e5090a8a-3522-4ac2-b391-e66d323bea91,DISK]]
1. BP-562833966-10.121.14.18-1548250294268:blk_1088363507_14657525 len=16777216 Live_repl=2  [DatanodeInfoWithStorage[10.121.14.9:9866,DS-f5f358b7-7b1a-434b-be5f-f9fdb711fae8,DISK], DatanodeInfoWithStorage[10.121.14.3:9866,DS-5df6dcc3-ad81-4dbf-8cff-9d0e572032cf,DISK]]
2. BP-562833966-10.121.14.18-1548250294268:blk_1088363508_14657526 len=16777216 Live_repl=2  [DatanodeInfoWithStorage[10.121.14.9:9866,DS-709bced4-8573-4a5a-a557-7b7e7c690472,DISK], DatanodeInfoWithStorage[10.121.14.8:9866,DS-0ba77bbb-4ffe-4e00-b92b-096b12e3b27c,DISK]]
3. BP-562833966-10.121.14.18-1548250294268:blk_1088363509_14657527 len=16777216 Live_repl=2  [DatanodeInfoWithStorage[10.121.14.14:9866,DS-417863e5-9075-4762-bbaa-ea4e6545011a,DISK], DatanodeInfoWithStorage[10.121.14.7:9866,DS-765ee11a-08d5-4778-9643-d5130b720a10,DISK]]
4. BP-562833966-10.121.14.18-1548250294268:blk_1088363510_14657528 len=61 Live_repl=2  [DatanodeInfoWithStorage[10.121.14.13:9866,DS-bb5cb880-4dae-4a3d-92da-ead25fca54ef,DISK], DatanodeInfoWithStorage[10.121.14.12:9866,DS-a79f886c-a581-4d27-840b-5c590707bd37,DISK]]


Status: HEALTHY
 Number of data-nodes:  14
 Number of racks:               1
 Total dirs:                    0
 Total symlinks:                0

Replicated Blocks:
 Total size:    67108925 B
 Total files:   1
 Total blocks (validated):      5 (avg. block size 13421785 B)
 Minimally replicated blocks:   5 (100.0 %)
 Over-replicated blocks:        0 (0.0 %)
 Under-replicated blocks:       0 (0.0 %)
 Mis-replicated blocks:         0 (0.0 %)
 Default replication factor:    3
 Average block replication:     2.0
 Missing blocks:                0
 Corrupt blocks:                0
 Missing replicas:              0 (0.0 %)

Erasure Coded Block Groups:
 Total size:    0 B
 Total files:   0
 Total block groups (validated):        0
 Minimally erasure-coded block groups:  0
 Over-erasure-coded block groups:       0
 Under-erasure-coded block groups:      0
 Unsatisfactory placement block groups: 0
 Average block group size:      0.0
 Missing block groups:          0
 Corrupt block groups:          0
 Missing internal blocks:       0
FSCK ended at Wed Nov 18 21:21:52 CET 2026 in 1 milliseconds


The filesystem under path '/user/xuedua076/e5/historico_16m.csv' is HEALTHY
[xuedua076@cdh61-login1 ~]$
```

```
```