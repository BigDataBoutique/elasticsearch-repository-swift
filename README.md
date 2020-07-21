Swift repository plugin for Elasticsearch
=========================================

In order to install the plugin, simply run: `bin/plugin install org.wikimedia.elasticsearch.swift/swift-repository-plugin/<version>`.

For Elasticsearch versions prior to 5.x, please refer to https://github.com/wikimedia/search-repository-swift.

Starting with version 3.0.0 of this plugin the plugin is developed on master, and compiled against all Elasticsearch versions in branches.

The resulting versioning schema looks like this: 3.0.0-es5.2.0, where the plugin version is followed by the ES version it is compatible with. A single plugin version will have several ES compatible versions.   

## Create Repository
```
    $ curl -XPUT 'http://localhost:9200/_snapshot/my_backup' -d '{
        "type": "swift",
        "settings": {
            "swift_url": "http://localhost:8080/auth/v1.0/",
            "swift_container": "my-container",
            "swift_username": "myuser",
            "swift_password": "mypass!"
        }
    }'
```
 
 
See [Snapshot And Restore](https://www.elastic.co/guide/en/elasticsearch/reference/current/modules-snapshots.html) for more information


## Settings
|  Setting                            |   Description
|-------------------------------------|------------------------------------------------------------
| swift_container                     | Swift container name. **Mandatory**
| swift_url                           | Swift auth url. **Mandatory**
| swift_authmethod                    | Swift auth method, one of "KEYSTONE" "TEMPAUTH" or "" for basic auth
| swift_password                      | Swift password
| swift_tenantname                    | Swift tenant name, only used with keystone auth
| swift_username                      | Swift username
| swift_preferred_region              | Region to use.  If you do not specify a region, Swift will pick the endpoint of the first region.  If you have multiple regions, the order is not guarenteed.
| chunk_size                          | Maximum size for individual objects in the snapshot. Defaults to `5gb` as that's the Swift default
| compress                            | Turns on compression of the snapshot files. Defaults to `false` as it tends to break with Swift
| max_restore_bytes_per_sec           | Throttles per node restore rate. Defaults to `20mb` per second.
| max_snapshot_bytes_per_sec          | Throttles per node snapshot rate. Defaults to `20mb` per second.

## Configuration Settings
Plugin settings to be placed in elasticsearch YAML configuration. Keep defaults, unless problems are detected.

|  Setting                            |   Description
|-------------------------------------|------------------------------------------------------------
| repository_swift.minimize_blob_exists_checks | true (default) or false. Reduces volume of SWIFT requests to check a blob's existence.
| repository_swift..allow_caching     | true or false (default). Allow JOSS caching

## To debug in Eclipse
Since Swift has logging dependencies you have to be careful about debugging in Eclipse.

1.  Import this project into Eclipse using the maven connector.  Do no import the main Elasticsearch code.
2.  Create a new java application debug configuration and set it to run ElasticsearchF.
3.  Go to the Classpath tab
4.  Click on Maven Dependiences
5.  Click on Advanced
6.  Click Add Folder
7.  Click ok
8.  Expand the tree to find <project-name>/src/test/resources
9.  Click ok
10. Click debug
