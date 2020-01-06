# DataObject

One of the key features of `primrose` is that it keeps data in memory. As data is shuffled among nodes for reading, processesing, modeling, and writing, it is readily available for nodes to use without unnecessary serialization and deserialization.

A central object receives new data, stores it, hands it to nodes that request it, and does all the book-keeping under the hood. This is the `DataObject`.

`DataObject` is a repository, a single instance that is handed to each node (in the sequence determined by the `DAGRunner`) to both add new data, but also to consume data, either reading it, transforming it, or popping it off the `DataObject` completely.

This concept is so central to `primrose` that is plays a pivotal role in the notion of a node and running a node. That is, `AbstractNode` has a method:

```
    def run(self, data_object):
        """
            run the node. For a reader, that means read, for a writer that means write etc.

        Args:
            data_object (DataObject): DataObject instance

        Returns:
           (tuple): tuple containing:

               data_object (DataObject): instance of DataObject

               terminate (bool): terminate the DAG?

        """
```

Every node in the DAG, receives the `DataObject`, runs (does whatever it needs to do) and returns that same DataObject. It might add new data, it might consume data, or it might just return it as is.

# Saving Data
`DataObject` is designed to hide some of the decisions and underlying book-keeping. If you want to save some data, you call the `add` method:

```
    data_object.add(self, some_object)
```

That's it! This will save the data `some_object`, associating it with the node that did the saving.

`some_object` can be anything at all. It might be a data frame, a string, a dictionary, any object that Python can handle.

For instance, a `CsvReader` might implement its `run` method as:
```
    filename = self.node_config['filename']
    logging.info('Reading {} from CSV'.format(filename))
    df = pd.read_csv(filename)
    data_object.add(self, df)
    terminate = df.empty
    return data_object, terminate
```
taking the filename listed in the config, reading into a pandas dataframe, and checking for empty data frame, and return `data_object`.

## Specifying a key
The data is saved with a default key (`DataObject.DATA_KEY`). However, you can use your own key:

```
    data_object.add(self, some_object, some_key)
```
such as
```
    data_object.add(self, df, "tennis_df")
```

This is important if you wish to save multiple objects:

```
    data_object.add(self, df1, "DF1")
    data_object.add(self, df2, "DF2")
```

although you could always save as a single dictionary:

```
    data_object.add(self, {"DF1": df1, "DF2": df2})
```

You have options.

# Getting Data
Typically, a node will need data that was stored in another node, upstream in the DAG. Thus, the typical action in a node is to save data (`data_object.add()` above) and/or to get upstream data. To do the latter, use `get_upstream_data`:

```
    data = data_object.get_upstream_data(self.instance_name,
                    pop_data=False,
                    rtype=DataObjectResponseType.KEY_VALUE.value)
```

The first argument is the name of the requestor (typically `self.instance_name`). Which node is requesting the data? This is used to determine which nodes are upstream of the focal node.

The `pop_data` flag determines whether you want to pop the data off the `DataObject`, oin which it will be garbge collected when no longer referenced. This is important as you might not want more and more data accumulating in memory.

## Specifying form of data
The last input to `get_upstream_data` is `rtype`, short for `DataObjectResponseType`, which specifies how you want the data delivered.

### Single objects
The simplest scenario is you just want a single object back. For instance, suppose the only upstream node was a reader that read data into a dataframe. You just want that data frame. In that case, you can specify

```
    rtype=DataObjectResponseType.VALUE.value
```

For instance,

```
    df = self.get_upstream_data(instance_name,
            rtype=DataObjectResponseType.VALUE.value)
```
will deliver the data to `df`.

If, however, there are multiple keys for the upstream node, instread of single data object, you will receive a dictionary of all the data, such `{"DF1": df1, "DF2": df2}`.

### Single dictionaries
A more complex scenario is if an upstream node saved multiple objects, such as

```
    data_object.add(self, df1, "DF1")
    data_object.add(self, df2, "DF2")
```

In this case, it is likely easiest to get a single dictionary back (especially if you don't know how many objects were saved and with what keys). For this, specify

```
    rtype=DataObjectResponseType.KEY_VALUE.value
```
This will return a single dictionary back, which in the case of the above example will be

```
    {"DF1": df1, "DF2": df2}
```
It will combine the separate, individual keys and objects into a single dictionary for you. To be clear, for this option, specify `KEY_VALUE` in `get_upstream_data`:

```
    data_dictionary = self.get_upstream_data(instance_name,
        rtype=DataObjectResponseType.KEY_VALUE.value)
```

### Set of Dictionaries
The last scenario is the most complex. Suppose that multiple upstream nodes feed into a node. For instance, perhaps 3 readers feed into a pipeline node. For this, you will want to get the data but associated with each upstream node; that is, a dictionary of instance_name keys and their data dictionaries. For this, specify

 ```
    rtype=DataObjectResponseType.INSTANCE_KEY_VALUE.value
```
This will return a single dictionary but where the keys are the names of the upstream nodes, and the values are dictionaries of the stored data and their associated key:

```
    {'instance_name': {'key': value}, 'instance_name2': {'key2': value2}, ... }
```

### Filtering data
You can also filter data for a given data key. You could request data with `KEY_VALUE` or `INSTANCE_KEY_VALUE` and inspect the key yourself but you can also use

```
    data_object.get_filtered_upstream_data(self, instance_name, filter_for_key)
```

where `filter_for_key` filters for data keys. For instance, with stored data

```
    {'instance_name': {'key': value}, 'instance_name2': {'key2': value2}, ... }
```
and calling

```
    data_object.get_filtered_upstream_data(self, instance_name, 'key2')
```
would return `{'key2': value2}`.

If you ask for `INSTANCE_KEY_VALUE` but there is a single key, you will still recive a dictionary keyed with instance name (`{'instance_name': {'key': value}`).

# Other methods
`DataObject` has other methods for inspecting data currently stored in `DataObject` such as `upstream_keys()` which provides the keys and `get()` which gets the data for a given instance name. It also handles local caching of data. However, the `add()` and `get_upstream_data()` are the methods most often used in nodes.

## Next
Learn more about Notifications: [Notifications](README_NOTIFICATIONS.md).