
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

## Paso 1 . Contorno

* El tañamo de bloque (en MB) sería de 128 MB
```
[xuedua076@cdh61-login1 ~]$ hdfs getconf -confKey dfs.blocksize
134217728
```

* Replicación
```
[xuedua076@cdh61-login1 ~]$ hdfs getconf -confKey dfs.replication
3
```

* Bytes por cada suma de verificación
```
[xuedua076@cdh61-login1 ~]$ hdfs getconf -confKey dfs.bytes-per-checksum
512
```
* Un bloque completo tiene 262144
```
134217728/512= 262144 sumas de verificación
```

* Carpeta personal en HDFS, la cual es 'e5' (volver a hacer una vez acabado)
```
[xuedua076@cdh61-login5 ~]$ hdfs dfs -ls
Found 1 items
drwxr-xr-x   - xuedua076 xunta          0 2026-11-18 21:15 e5
[xuedua076@cdh61-login5 ~]$
```

* El clúster tiene una capacida de 2.5p y actualmente tiene un 11% ocupado (2.2P de espacio libre)
```
[xuedua076@cdh61-login1 ~]$ hdfs dfs -df -h
Filesystem            Size     Used  Available  Use%
hdfs://nameservice1  2.5 P  283.2 T      2.2 P   11%
```

## Paso 2 . Subir fichero y ver bloques

```
PRUEBA ELIMINAR LUEGO
[xuedua076@cdh61-login1 ~]$ ls
reservas_historico.csv  xera_historico.py
```

```
PRUEBA ELIMINAR LUEGO

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

* Cantidad de bloques en cada fichero 
    * En el caso de historico_def al tener la configuración por defecto cada bloque sería de 128 MB teniendo como resultado 1 solo bloque , esto debido a que el fichero ocupa solo 64 MB
    * Pero en el csv llamado historico_16m se ha creado con bloques de 16 MB cada uno , debido a esto dividiremos esa información en varios bloques (4 de 128 MB y el último ocupando 61 bytes)
         * Esto ya que 67108925 x 128/134217728 no son 64 MB exactos (Si 134.217.728 bytes son 128 MB, entonces 67.108.925 bytes son), sobrarían 0,0000581741333 MB los cuales al pasarlos a bytes mutiplicandolos por (x1024x1024) serían 61 bytes
```
[xuedua076@cdh61-login1 ~]$ hdfs dfs -stat "%n · %b bytes · bloque %o · replicación %r" e5/*
historico_16m.csv ?? 67108925 bytes ?? bloque 16777216 ?? replicaci??n 3
historico_def.csv ?? 67108925 bytes ?? bloque 134217728 ?? replicaci??n 3
```
* Un bloque de historico_16m.csv
    * En uno de los bloques de historico_16m llamado blk_1088363506_14657524 sus copias están en los datanodes : 3,13,14
    * Encontramos que están en datanodes distintos debido a la configuración por defecto de hdfs y el factor de replicación 3


```
0. BP-562833966-10.121.14.18-1548250294268:blk_1088363506_14657524 len=16777216 Live_repl=3  [DatanodeInfoWithStorage[10.121.14.3:9866,DS-cef69aab-1919-4780-9a18-66ae0fca7730,DISK], DatanodeInfoWithStorage[10.121.14.13:9866,DS-9c46ff26-5bfe-4c47-8912-87a3d979bfb8,DISK], DatanodeInfoWithStorage[10.121.14.14:9866,DS-e5090a8a-3522-4ac2-b391-e66d323bea91,DISK]]

```

* En el resúmen de fsck nos muestra que en el clúster tenemos 14 datanodes

```
Status: HEALTHY
 Number of data-nodes:  14
```

* La primera cifra nos muetsra el tamaño real del fichero y la segundo indica lo que ocupa con sus réplicas 64x3 (RF3)

```
[xuedua076@cdh61-login1 ~]$ hdfs dfs -du -h e5
64.0 M  192.0 M  e5/historico_16m.csv
64.0 M  192.0 M  e5/historico_def.csv
```

## Paso 3 . Replicación

```
[xuedua076@cdh61-login1 ~]$ hdfs dfs -setrep 2 e5/historico_16m.csv
Replication 2 set: e5/historico_16m.csv
```
* Cambios en fsck 
    * En la sálida donde nos muestra los bytes y diferentes datos ahora nos muestra un replicated de replication=2
    * Al mostrar en que datanodes tienen las réplicas cada bloque ,encontramos que ahora solo se encuentran en 2 datanodes. Por ejemplo en el blk_1088363506_14657524 ahora solo en vez de mostrar 3,13 y 14 solo muestra 3 y 14
    * Nos muestra que ahora la media de copias guardadas por blqoue son 2 ( Average block replication:2.0)

```
[xuedua076@cdh61-login1 ~]$ hdfs fsck /user/xuedua076/e5/historico_16m.csv -files -blocks -locations
...

/user/xuedua076/e5/historico_16m.csv 67108925 bytes, replicated: replication=2, 5 block(s):  OK

0. BP-562833966-10.121.14.18-1548250294268:blk_1088363506_14657524 len=16777216 Live_repl=2  [DatanodeInfoWithStorage[10.121.14.3:9866,DS-cef69aab-1919-4780-9a18-66ae0fca7730,DISK], DatanodeInfoWithStorage[10.121.14.14:9866,DS-e5090a8a-3522-4ac2-b391-e66d323bea91,DISK]]

...

 Average block replication:     2.0
```
 * Fsck después de pedir más copias que datanodes
    * Para cada uno de los bloques ya nos muestra los datanodes ,si no un mensaje que nos indica que no tiene suficientes nodos para hacer la réplica 15 ,ya que no pueden repetirse datanodes en el mismo bloque 
    * under-replicated nos indica que tiene menos copias activas que las que tiene configuradas
    * El estado sigue siendo healthy porque las copias pueden seguir generándose ya que sigue teniendo las mínimas necesarias
```
/user/xuedua076/e5/historico_16m.csv:  Under replicated BP-562833966-10.121.14.18-1548250294268:blk_1088363506_14657524. Target Replicas is 15 but found 14 live replica(s), 0 decommissioned replica(s), 0 decommissioning replica(s).
```
* El namenode lo daría por muerto después de no tener latidos por 10 minutos y ordenaría que los otros nodos del bloque se reorganicen y envíen una copia de lo que falta a otro datanode del clúster


## Paso 4 . Sumas de verificaciones

* Apartado 1 
    * Los resumenes son diferentes ya que el calculo depende del tamaño bloque , en uno está dividido en bloques de 16 MB y en otro de 128 MB 
    * Significado de cada número :
        * CRC32C cada 512 B
        * 32768MD5 : número de sumas CRC32C que caben en un  bloque de 16 MB
        * En el caso de historico_def, nos muestra un 0 ya que el yamaño del fichero cabe en un bloque completo de 128 MB , pero como no lo completa no lo suma
```
[xuedua076@cdh61-login5 ~]$ hdfs dfs -checksum e5/historico_16m.csv e5/historico_def.csv
e5/historico_16m.csv    MD5-of-32768MD5-of-512CRC32C    0000020000000000000080006ae2f7f2b5c099a73b4a2eed8ab700ed
e5/historico_def.csv    MD5-of-0MD5-of-512CRC32C        000002000000000000000000b349a5b4908740c0dfa3a87336a9846b
```
* Apartado 2
    * Al realizar la comparación del resumen con el modo para no depender del tamaño del bloques se nos muestar el mismo resumen , ya que tienen el mismo contenido 
    * Permite verificar que aunque se hayan guardado con diferentes tamaños de bloques siguen teniendo el mismo contenido y no se ha perdido ni corrompido ningun dato .
```
[xuedua076@cdh61-login5 ~]$ hdfs dfs -Ddfs.checksum.combine.mode=COMPOSITE_CRC -checksum e5/historico_16m.csv e5/historico_def.csv
e5/historico_16m.csv    COMPOSITE-CRC32C        5f0623f9
e5/historico_def.csv    COMPOSITE-CRC32C        5f0623f9
```

* Apartado 3 
    * La copia del paso 3 tiene el mismo resumen que historico_def.csv en el paso 1 ya que al no que se quería conservar el mismo tamaño de bloque que el original (16 MB) , se ha creado con la configuración por defecto que es de (128 MB)

```
[xuedua076@cdh61-login5 ~]$ hdfs dfs -cp e5/historico_16m.csv e5/copia.csv
[xuedua076@cdh61-login5 ~]$ hdfs dfs -checksum e5/historico_16m.csv e5/copia.csv
e5/historico_16m.csv    MD5-of-32768MD5-of-512CRC32C    0000020000000000000080006ae2f7f2b5c099a73b4a2eed8ab700ed
e5/copia.csv    MD5-of-0MD5-of-512CRC32C        000002000000000000000000b349a5b4908740c0dfa3a87336a9846b
```

* Apartado 4 
    * El resúmen muestra que ha cambiado por completo ,ya que se modifica por la suma de esos 512 bytes lo que cambia el resultado final

```
[xuedua076@cdh61-login5 ~]$ cp reservas_historico.csv modificado.csv
[xuedua076@cdh61-login5 ~]$ printf 'X' | dd of=modificado.csv bs=1 seek=1000000 conv=notrunc
1+0 records in
1+0 records out
1 byte (1 B) copied, 0.000480298 s, 2.1 kB/s
```

```
[xuedua076@cdh61-login5 ~]$ hdfs dfs -D dfs.blocksize=16m -put modificado.csv e5/modificado.csv
[xuedua076@cdh61-login5 ~]$ hdfs dfs -checksum e5/historico_16m.csv e5/modificado.csv
e5/historico_16m.csv    MD5-of-32768MD5-of-512CRC32C    0000020000000000000080006ae2f7f2b5c099a73b4a2eed8ab700ed
e5/modificado.csv       MD5-of-32768MD5-of-512CRC32C    00000200000000000000800002b51d0ccda7017aa5616b99f7dde5f4
```
* Apartado 5
    * Es necesario descargarlo ya que para hacer la misma operación hdfs(-checksum) y linux(sha256sum) utilizan diferentes métodos .
    * También se tiene en cuenta que uno está divido en bloques guardado en hdfs , debido a esto se descarga para tenerlo en la misma máquina local de linux , compararlo con el mismo método y comprobar que son idénticos

```
[xuedua076@cdh61-login5 ~]$ hdfs dfs -get e5/historico_16m.csv descargado.csv
[xuedua076@cdh61-login5 ~]$ sha256sum reservas_historico.csv descargado.csv
43647ae2b81ea1ba02edc3819f81327872c769db91e9ccf30d0677e2cff34891  reservas_historico.csv
43647ae2b81ea1ba02edc3819f81327872c769db91e9ccf30d0677e2cff34891  descargado.csv
```

## Paso 5. Preguntas 
*  Pregunta 1:
    * Si es mayor el volúmen tenemos que tener más precaución con su integridad ya que es más probable que se produzcan fallos en los discos .
    * Normalmente de 125 Tb unos 12 van a estar mal , la proporción se aumenta si tenemos un volúmen mayor ya que con más frecuencia se van a estropear los discos 

*  Pregunta 2:
    * Al escribir un bloque lo comprueba el último datanode de la canalización
    * Al leer el cliente , que recalcula los CRC y los compara
    * Cuando nadie lee lo comprueba cada datanode con su block scanner que lee los bloques periodicamente

*  Pregunta 3:
    1. Al detectarse una réplica corrupta debido a la suma de la verificación ,se avisa al namenode que la marque como tal
    2. El cliente mientras lee de otra réplica
    3. El namenode ordena copiar una réplica no corrupta a otro nuevo datanode
    4. Cuando se han generado suficientes copias, se borra la corrupta
    
*  Pregunta 4:
    * Cuando todas las réplicas están corruptas fsck mostra CORRUPT y se pierde ese bloque y dejan de existir copias disponibles
    * Si necesitasemos recuperar ese fichero no podría ser por hdfs se tendría que subir el fichero desde ña fuente original

*  Pregunta 5:
    * Copias
        * En HDFS permite recuperar los bloques si falla algún datanode ,ya que existe una copia de estos en otro nodo
        * En garage sirve para poder seguir accediendo a esos datos si una de las zonas falla, ya que cada copia de una zona está en otra distinta
    
    * Resumenes
        * En HDFS tienen el papel de comprobar que no se ha corrompido ningún dato comparandolo con el original
        * En garage permite verificar cuando una copia es correcta comparando su contenido con el propio resumen, si no es correcta se reemplaza con una correcta.
