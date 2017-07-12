# RIPE-RDImporter
**R**éseaux **IP** **E**uropéens **R**egistry **D**ata **I**mport tool. 

Provided that you have downloaded [RIPE]'s DB snapshots of [INETNUM] and [ORGANIZATION] entities, representing individual CIDR networks and owning organizations respectively, this tool will scan a specified number of lines for networks, parse its accompaning information and build a complete record including referential organization data.

[RIPE]: https://www.ripe.net
[INETNUM]: http://ftp.ripe.net/split/ripe.db.inetnum.gz
[ORGANIZATION]: http://ftp.ripe.net/split/ripe.db.organisation.gz

### Sample Output
```
...
SA; ORG-SSTC1-RIPE; 39321600; 39583743; 2.88.0.0/14; NULL; "SA-STC-20100517"; "LIR"; "Saudi Telecom Company JSC"
...
SA; NULL; 39490816; 39501823; 2.90.149.0/24; "DSL customers"; "SAUDINET_DSL"; NULL; NULL
SA; NULL; 39501824; 39518207; 2.90.192.0/18; "SaudiNet DSL pool_Dynamic IPs"; "SAUDINET_DSL_POOL"; NULL; NULL 
...
RU; ORG-CTaM1-RIPE; 39583744; 39845887; 2.92.0.0/14; NULL; "RU-CORBINA-20100521"; "LIR"; "OJSC "Vimpelcom""
RU; NULL; 39714816; 39845887; 2.94.0.0/15; "Dynamic IP Pool for Broadband Customers"; "BEELINE-BROADBAND"; NULL; NULL
...
```

## Contents
1. [Dependencies & Installation](#dependencies--installation)
2. [Configuration](#configuration)
3. [Execution](#execution)
4. [Output](#output)
5. [Vertica & Analysis](#vertica-import--analysis)

## Dependencies & Installation
* [ipcalc] (Python 2.x+)
* [netaddr] (Python 2.5+)

`pip install ipcalc netaddr`

[ipcalc]: https://github.com/tehmaze/ipcalc
[netaddr]: https://github.com/drkjam/netaddr

## Configuration
All options are set via **config.ini** and validated upon program start. Errors are presented accordingly. For empty values, please set `Option = ` or `Option = None`.

### Task Settings
|Option                     |Type   |Required |Format                 |Example (Default)  |
|------                     |----   |:------: |------                 |------             |
|LinesToProcess<sup>1</sup> |Int    |No       |Decimal                |`1000`             |
|ColumnDelimiter<sup>2</sup>|Char   |Yes      |-                      |`;`                |
|ParseTask                  |Bool   |No       |True, False            |`(True)`           |
|PostProcessTask            |Bool   |No       |True, False            |`(False)`          |
|FileDateForPostProcess     |String |No       |Datestring (mm_dd_YYYY)|`6_28_2017 (today)`|

<sup>1</sup> Set `LinesToProcess = None` or `LinesToProcess = ` to process complete file. **Note:** May take several hours (~6h).

<sup>2</sup> Set `ColumnDelimiter = Unicode` for pre-specified unicode character `\024`. Will most likely be unique to data set, as it represents escape key.

### Directory Settings<sup>3</sup>
|Option                     |Type   |Required |Example  |
|------                     |----   |:------: |------   |
|RegistryDataDirectory      |String |Yes      |`data/`  |
|OutputDirectory            |String |Yes      |`output/`|

<sup>3</sup> Trailing slash will be appended if missing. **Note:** Follows path from project root.

### Output Settings
|Option                     |Type   |Required |Example/Default                            |
|------                     |----   |:------: |------                                     |
|FileBaseRegistryData       |String |No       |`ripe.db`                                  |
|FileBaseOutput             |String |No       |`ripe_registry_data`                       |
|FileBaseFailedLookup       |String |No       |`ripe_registry_failed_organisation_lookups`|
|FileBaseExceptions         |String |No       |`ripe_registry_exceptions`                 |

## Execution
```
cd RIPE-RDImporter/
python RDImporter.py
```

1. Load settings
2. Parse + output registry data

**Note:** Always scans a specified number of lines from the beginning of an INETNUM file.

3. Post-proces parsed data

**Note:** Removes records not managed by RIPE, including special purpose networks.

## Output
Each line-break `'\n'` seperated record in the produced output file consists of the following fields:

|Field        |Format                 |Guaranteed |Example                            |
|------       |----                   |:------:	  |----                               |
|country_code |ISO3166-1              |No	  |`DK`                               |
|org_code     |String                 |No	  |`ORG-EA44-RIPE`                    |
|start_ip     |Dotted Decimal Notation|Yes 	  |`213.159.160.0`                    |
|end_ip       |Dotted Decimal Notation|Yes	  |`213.159.191.255`                  |
|ip_prefix    |CIDR Notation          |Yes        |`213.159.160.0/19`                 |
|descr        |String                 |No         |`NULL`                             |
|netname      |String                 |No         |`SE-ERICSSON-20010504`             |
|org_type     |String                 |No         |`LIR`                              |
|org_name     |String                 |No         |`Telefonaktiebolaget L M Ericsson` |

## Vertica Import & Analysis
#### Create corresponding table via DDL:
```
DROP TABLE IF EXISTS ripe_registry_data CASCADE;
CREATE TABLE ripe_registry_data (
	"country_code" varchar(10),
	"org_code" varchar (100),
	"start_ip" integer PRIMARY KEY,
	"end_ip" integer NOT NULL,
	"ip_prefix" varchar (100),
	"descr" varchar(100),
	"netname" varchar(100),
	"org_type" varchar(100),
	"org_name" varchar(100)
	-- "asn" varchar(100),
	-- "as_descr" varchar(100)
);
```

#### Import produced output files via COPY DIRECT: 
```
COPY ripe_registry_data (
	country_code,
	org_code,
	orginal_start_ip FILLER VARCHAR,
	original_end_ip FILLER VARCHAR,
	start_ip as INET_ATON(orginal_start_ip),
	end_ip as INET_ATON(original_end_ip),
	ip_prefix,
	descr,
	netname,
	org_type,
	org_name
	-- asn,
	-- as_descr
)
FROM LOCAL '../Parsed-RIPE-Data/ripe_registry_post_6_27_2017.txt'
DELIMITER E'\024'
NULL 'NULL'
REJECTED DATA 'error/ripe_rejected_ips.txt'
EXCEPTIONS 'error/ripe_exceptions.txt'
DIRECT;
```

**Note:** Adjust delimiter to match the configured column delimiter.

#### Retrieve percentage of IPv4 address space covered by imported data:
```
SELECT ((sum((end_ip+1)-start_ip))/(4*1024*1024*1024))*100.0 as coverage_percentage
FROM ripe_registry_data
WHERE org_type = '"LIR"';
```

**Note:** Predicate `WHERE org_type = 'LIR'` is necessary to combat overlapping data (comp. [Sample Output](#sample-output)). For exact coverage, subtract number of special purpose IPs from total IP count (2^32):

```
SELECT ((sum((end_ip+1)-start_ip))/(4*1024*1024*1024-(
	2*(1024*1024*256)+
	3*(1024*1024*16)+
	1*(1024*1024*4)+
	1*(1024*1024)+
	1*(1024*128)+
	2*(1024*64)+
	5*(256)+
	1
)))*100.0 as coverage_percentage
FROM ripe_registry_data
WHERE org_type = '"LIR"';
```
