# NASA PDS Registry Loader

Starting with release 0.5.1 of the Registry API, an _archive status_ field was introduced which enables the selective 
availability of data based on its value for each product, collection and bundle. Archive status may be assigned a value of; 
_staged_, _restricted_, _certified_ and _archived_. Only those records with an archive status of _archived_ are visible 
through the API.

The script contained in this repository; _upgrade/archive-status.sh_ introduces this field to all records stored within
the indicated ElasticSearch/Opensearch endpoint, setting all values to _staged_. **If you are upgrading the Registry API 
from any release before 0.5.1 to 0.5.1 or later, this script must be applied to your Registry's Opensearch endpoint.**

The syntax for invoking this script is as follows:

```archive-status.sh <Opensearch URL> [<Opensearch username> <Opensearch password>]```

Once this script has been applied, the _registry-manager_ utility is used to promote the archive status of the 
products, collections and bundles to other values. Refer to the 
[_registry-manager_ documentation](https://nasa-pds.github.io/pds-registry-app/operate/reg-manager.html#ArchiveStatus) 
on using the set-archive-status command.

## Contributing

Within the NASA Planetary Data System, we value the health of our community as much as the code. Towards that end, we ask that you read and practice what's described in these documents:

 -   Our [contributor's guide](https://github.com/NASA-PDS/.github/blob/main/CONTRIBUTING.md) delineates the kinds of contributions we accept.
 -   Our [code of conduct](https://github.com/NASA-PDS/.github/blob/main/CODE_OF_CONDUCT.md) outlines the standards of behavior we practice and expect by everyone who participates with our software.

## License

The project is licensed under the [Apache version 2](LICENSE.md) license.


