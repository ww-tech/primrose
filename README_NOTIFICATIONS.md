# Notifications

A useful feature of `primrose` is the ability to get notified when your job is complete (or at any node, for that matter) and/or if there were errors in running your DAG. These two cases are handled slightly differently. Note that the current implementation is only set up to handle [Slack](https://slack.com/intl/en-ca/) notifications.

To get started, you'll want to create a [`Slack App`](https://api.slack.com/apps?new_app=1) if you don't already have one. You'll be given an `OAuth Access Token`, and you'll have to choose a `slack channel` associated to the app. This is where your notifications will be posted.

## DAG Notifications

To get notified at points in your job, create a notification node at the desired stage in your configuration file. For example, below we implement a simple job to read a `.csv` file and post a success message upon completion.

```
implementation_config:
  reader_config:
    read_data:
      class: CsvReader
      filename: data/tennis.csv
      destinations:
        - notification
  cleanup_config:
    notification:
      class: ClientNotification
      client: SlackClient
      channel: my-channel
      token: slack-token
      message: We did it!
```

This DAG will read the file `data/tennis.csv` and store it in the `data_object` as a pandas dataframe. It will then post the message "We did it" in the `my-channel` slack channel. If you would like to get pinged on the notification, add your `member/user_id` as a parameter. Note that this is NOT your user name, but a string of the format "W012A3CDE" typically beginning with "U" or "W". The notification portion of your DAG would then look like

```
notification:
    class: ClientNotification
    client: SlackClient
    channel: my-channel
    token: slack-token
    member_id: W012A3CDE
    message: We did it!
```

## Error Notifications

To get notified if your job fails at any point in the DAG, you will need to create a `notify_on_error` key in the `metadata` section of the DAG. As above, this will require specifiying the `client`, `channel`, and `token`. `member_id` is optional. An example configuration would like this

```
metadata:
  notify_on_error:
    client: SlackClient
    channel: my-channel
    member_id: W012A3CDE
    token: slack-token
implementation_config:
  reader_config:
    read_data:
      class: CsvReader
      filename: data/tennis.csv
```

The  `notify_on_error` key triggers the DAG runner to instantiate your client with the given parameters, and will post to `my-channel` upon hitting an error.

## Environment Variables

If you would (understandably) prefer to have your configuration file read in the values of the client's parameters using environment variables, store your environment variables in the form "{CLIENT_NAME}_{CLIENT_KEY}". For example, for the `primrose` `SlackClient`, our variables would be stored as
```
SLACKCLIENT_TOKEN="some-token"
SLACKCLIENT_CHANNEL="some-channel"
SLACKCLIENT_MEMBER_ID="USomeUserID"
```

Our configuration file would then look something like

```
metadata:
  notify_on_error:
    client: SlackClient
    channel:
    member_id:
    token:
implementation_config:
  reader_config:
    read_data:
      class: CsvReader
      filename: data/tennis.csv
      destinations:
        - notification
  cleanup_config:
    notification:
      class: ClientNotification
      client: SlackClient
      token:
      channel:
      message: We did it!
```
The necessary keys to instantiate the client must be listed, and their values must be empty.
