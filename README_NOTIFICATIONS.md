# Notifications

A useful feature of `primrose` is the ability to get notified when your job is complete (or at any node, for that matter) and/or if there were errors in running your DAG. These two cases are handled slightly differently. Note that the current implementation is only set up to handle [Slack](https://slack.com/intl/en-ca/) notifications.

To get started, you'll want to create a [`Slack App`](https://api.slack.com/apps?new_app=1), if you don't already have one. The Slack [docs](https://api.slack.com/authentication/basics) go through the setup in detail. This can then be used by your whole team and added to multiple channels. If you are using an existing app, you will need the `User OAuth Token`.
A recent update in Slack ensures slack apps act independently of an installing user, therefore deactivation of an installing user no longer has an effect on the app, see [here](https://api.slack.com/authentication/quickstart#deactivation).

If you are creating your own app, navigate to `OAuth & Permissions` in the sidebar and scroll to the `Scopes` section. Click on `Add an OAuth Scope` to add your scopes. You will want to include the `chat:write:bot` scope to allow your app to post messages, and the `incoming-webhook` scope to allow messages to be posted in specific channels. You may need Admin approve if the app is installed in a particular workspace. When "installing" your app, you'll have to choose a `slack channel` associated to the app. This is where your notifications will be posted. You can also do this directly in Slack by navigating to your channel `Details`, clicking on the `More` option, and choosing `Add apps` from the drop down. One more thing needs to be done is to invite the bot into the channel, simply go to your Slack Channel and type the following as a message `/invite @primrose`.


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
      channel: name-of-channel
      token: slack-token
      message: We did it!
```

This DAG will read the file `data/tennis.csv` and store it in the `data_object` as a pandas dataframe. It will then post the message "We did it" in the `name-of-channel` slack channel. If you would like to get pinged on the notification, add your `member_id` as a parameter. Note that this is NOT your user name, but a string of the format "W012A3CDE" typically beginning with "U" or "W". You can find your slack `member ID` by navigating to your `Profile` and choosing the `More` option. You will see your `member ID`in the drop down.

The notification portion of your DAG would then look like

```
notification:
    class: ClientNotification
    client: SlackClient
    channel: name-of-channel
    token: slack-token
    member_id: W012A3CDE
    message: We did it!
```

## Error Notifications

To get notified if your job fails at any point in the DAG, you will need to create a `notify_on_error` key in the `metadata` section of the DAG. As above, this will require specifying the `client`, `channel`, and `token`. `member_id` and `message` are optional. An example configuration would look like this

```
metadata:
  notify_on_error:
    client: SlackClient
    channel: name-of-channel
    member_id: W012A3CDE
    token: slack-token
    message: Job Name 123
implementation_config:
  reader_config:
    read_data:
      class: CsvReader
      filename: data/tennis.csv
```

The  `notify_on_error` key triggers the DAG runner to instantiate your client with the given parameters, and will post to `name-of-channel` upon hitting an error.  Specifically, it will preface the error message with the value of `message` in the your `notify_on_error` keys (this is defaulted to `Job error` if you leave out the `message` key). For example, if `data/tennis.csv` does not exist, your app will post `Job Name 123: Issue with read_data`, followed by the traceback message. This is useful in identifying which of your DAGs has the error if you have multiple jobs posting to the same channel.

## Environment Variables

If you would (understandably) prefer to have your configuration file read in the values of the client's parameters using environment variables, store your environment variables in the form "{CLIENT_NAME}_{CLIENT_KEY}". For example, for the `primrose` `SlackClient`, our variables would be stored as
```
SLACKCLIENT_TOKEN="some-token"
SLACKCLIENT_CHANNEL="some-channel-name"
SLACKCLIENT_MEMBER_ID="USomeUserID"
```

Our configuration file would then look something like

```
metadata:
  notify_on_error:
    client: SlackClient
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
      message: We did it!
```
