# *RIPE-RDImporter*
**R**éseaux **IP** **E**uropéens **R**egistry **D**ata **I**mport tool.

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
|Option                     |Type   |Required |Format                 |Example/Default                            |
|------                     |----   |:------: |------                 |------                                     |
|FileBaseRegistryData       |String |No       |-                      |`ripe.db`                                  |
|FileBaseOutput             |String |No       |-                      |`ripe_registry_data`                       |
|FileBaseFailedLookup       |String |No       |-                      |`ripe_registry_failed_organisation_lookups`|
|FileBaseExceptions         |String |No       |-                      |`ripe_registry_exceptions`                 |

## *Execution*
```
cd RIPE-RDImporter/
python RDImporter.py
```

1. Loading of settings
2. Parse task
3. Post-process task

## *Output*
Each line-break seperated record in the produced output file consists of the following fields:

|Field        |Format                 |Guaranteed |Example                            |
|------       |----                   |:------:   |----                               |
|country_code |ISO3166-1              |No         |`DK`                               |
|org_code     |String                 |No         |`ORG-EA44-RIPE`                    |
|start_ip     |Dotted Decimal Notation|Yes        |`213.159.160.0`                    |
|end_ip       |Dotted Decimal Notation|Yes        |`213.159.191.255`                  |
|ip_prefix    |CIDR Notation          |Yes        |`213.159.160.0/19`                 |
|descr        |String                 |No         |`NULL`                             |
|netname      |String                 |No         |`SE-ERICSSON-20010504`             |
|org_type     |String                 |No         |`LIR`                              |
|org_name     |String                 |No         |`Telefonaktiebolaget L M Ericsson` |

## *Vertica Import & Analysis*
Create corresponding table via VSQL DDL:
```
```

Import produced output files via VSQL COPY DIRECT: 
```
```

Retrieve IPv4 percentage covered by imported data:
```
```

