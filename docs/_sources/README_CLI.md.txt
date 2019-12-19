# Primrose Command Line Interface

`primrose` contains a command line interface (CLI) to run a number of tasks.

## Getting help
To get help and see the list of commands available, run

```
  primrose --help
```
and you will see
```
Usage: primrose [OPTIONS] COMMAND [ARGS]...

  This is the command line interface for primrose. The command most people
  need is `primrose run` to run a primrose job. Type primrose commandname
  --help for more detailed help on a command

Options:
  --help              Show this message and exit.

Commands:
  create-project                  Create template project filled with...
  generate-class-registration-template
                                  Create template to register your own...
  generate-run-script             Create primrose run script in your...
  plot                            Create an image of the DAG
  run                             Run a primrose job
  validate                        Validate a primrose config
```
To get more details on a command type `primrose`, name of command followed by `--help`. For instance, to get more help on the `run` command, type `primrose run --help` and you will see:

```
Usage: primrose run [OPTIONS]

  Run a primrose job

Options:
  --config TEXT   Path to Config file  [required]
  --dry_run TEXT  Config file
  --help          Show this message and exit.
  ```

## primrose run
You've already seen the most important command:
```
   primrose run --config path/to/config
```
which runs a `primrose` job.

You can also run the job in `dry_run` mode which sets out the sequence of which nodes will run in which order but doesn't actually run the job. For this, just add the `--dry_run true` argument:

```
   primrose run --config path/to/config --dry_run true
```

## primrose create_project
To start with a blank primrose project, you use the command:
```
    primrose create-project --name my_project_name
```
Which will build a directory, `my_project_name` with the necessary files to start working with `primrose`.


## primrose validate
The first part of running a job is to validate the configuration file. However, if you want to do that and only that, you can run the `primrose validate` command:

```
  primrose validate --config path/to/config
```

## primrose plot
You've also seen 
```
  primrose plot --config path/to/config --outfile path/to/output/graph.png
```
There are many other arguments to control node size, text angles and the like. See `primrose plot --help` for full details.

## Other commands
The last two commands `primrose generate-class-registration-template` and `primrose generate-run-script` relate to 
extending `primrose` and using within your own project. This is covered in more detail in the [Developer Notes](README_DEVELOPER_NOTES.md).

## Specifying your primrose node registry
See [Developer Notes - Register your classes](README_DEVELOPER_NOTES.md#step-2-register-your-classes) for information about registering custom `primrose` nodes

## Next
Learn how to create your own `primrose` nodes: [Developer Notes](README_DEVELOPER_NOTES.md)