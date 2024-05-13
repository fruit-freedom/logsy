Architecture
------------

Services: `logsy`, `logsy-server`, `logsy-agent`, `logsy-web`


Supported types
---------------

- JSON
- Image
- XYZ




Create objects
--------------

`algorithm_name`: str
`meta`: JSON
`file` or `filepath`



Events
------

```json
{
    "timestamp": "",
    "type": "task:created | task:updated | object:created | object:updated",
    "instance": {
        /// Updated Task or Object instance
    }
}
```

TODO
----

1. [+] Task to Object use Many to Many
2. [+] RabbitMQ events
3. [] Add groups for objects
4. [] Add `Object` to Python SDK + Object as task input
5. [] Support for geotiff
6. [] Preprocessing for images + previews
7. [+] Initiate `logsy-agent`
