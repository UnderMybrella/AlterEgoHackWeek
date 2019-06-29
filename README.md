# Alter Ego - Hack Week

A rewrite of my 'Alter Ego' bot from the /r/danganronpa server, for Hack Week. The default prefix is "~|", but Alter Ego also responds to mentions.

## Running
Check the releases page for the jar file. Tested on Java 8.
Run like so: `java -jar AlterEgoHackWeek.jar`.

You'll need a file called `config.properties` in the directory that you run the jar from. A sample is included.

Parameters marked with brackets `[like this]` are mandatory, braces `{like this}` are optional. Parameters that have spaces in them must be enclosed in `"quotation marks like this"`

## Modules

### Auto Slow Mode
This module allows you to set up spam filters to automatically kick in when x messages are sent over y seconds, and locks the channel down with Discord's "slow mode", for as long as you deem necessary.

#### Syntax
~|configure slowdown for [channel ID or name] [key] [value]

[key] can be one of 'threshold', 'period', 'limit', 'duration', 'enabled_text', or 'disabled_text'.

'Threshold' is how many messages are required to turn slowmode on, 'period' is the period of time to judge message count (in milliseconds!), 'limit' is the rate limit on the channel (in seconds!), 'duration' is how many (milliseconds) the slow mode lasts for, 'enabled_text' is sent to the channel when slow mode is enabled, and 'disabled_text' is sent when slow mode is turned off.

~|remove slowdown for [channel ID or name]

Removes the slowdown on the channel

~|show slowdown for [channel ID or name]

Shows the slowdown on a channel, and explains the elements

### Meta Module
This module controls a few things about Alter Ego for your server

#### Syntax
~|meta change prefix [prefix]

Changes the prefix for this server.

~|meta change command [command name] [command trigger]

Changes the trigger that this bot should respond to. Use the ~|help command for a list of command names.

### Roles Module
This module is designed to make the lives of admins easier with regards to role management. Roles can be bundled together and can be requested and assigned via reactions.

#### Syntax
~|roles create bundle [assignable|requestable] [role ID or name] [regex] {additional role IDs}
This creates a role bundle that can either be assigned or requested. It adds an emote that matches the pattern of `AE_Assign_[Role Name]` with the colour of the role, and when someone sends a message that matches the supplied regex it adds the reaction to their message.
If the role bundle is designated as assignable, then the author can add the reaction themselves and give (or take) the role away. If the role is requestable, someone with the `MANAGE_ROLES` permission must add the reaction.

Any additional role IDs supplied will also be added

~|roles remove bundle [assignable|requestable] [role ID or name] [regex]
Removes a bundle that was assignable or requestable, that would give the role when a message with this regex was sent.

~|roles list bundles
Lists out the bundles available

~|roles add exclusive [first role] [second role] {third or more roles}
Designates a set of roles as "exclusive". Exclusive roles cannot coexist on a member, and if one is added then the others are taken away. So if the Red, Green, and Blue roles are set to be exclusive, and a user has the Green role and they're given the Red role, they lose the Green role.

~|roles remove exclusive [first role] [second role] {third or more roles}
Designates a set of roles to no longer be "exclusive."

~|roles list exclusive
Lists the roles that have been designated as exclusive for this server

### Users Module
Users come and go, a lot, sometimes. And sometimes, users might try to evade punishment by leaving and rejoining a server.
Fear no more. Alter Ego remembers what roles someone had, and if a safety net is set up it can readd them - with an optional delay, too!

#### Syntax
~|users change greeting [channel] [greeting] {newbie role}
Changes the greeting setting for this server. When a user joins, after a few seconds they'll be sent a personalised message to [channel] with the contents of [greeting].
Greetings support a few template variables:
- %user.mention: The users mention
- %user.name: The users name
- %user.id: The users ID 
- %time: The join time

If {newbie role} is defined, then that role will be given to all newbies as well

~|users remove greeting
Stop greeting new users. How sad :(

~|users show greeting
Show details about the greetings for this server, as well as a template of what it looks like.

~|users change safety net [info_channel | delay | allow_emoji | deny_emoji] [value]
Change a value about the safety net.
- info_channel is a channel where some details about when a user joined, when their account was created, when they last left (if ever), and if they had old roles. This is necessary to be able to **deny** users their old roles upon rejoining.
- delay: How long to wait before sending the message, and automatically giving a user their roles back. Set to -1 to never give them back automatically.
- allow_emoji: The emoji to use to signify to give a user their roles back
- deny_emoji: The emoji to use to signify to not give a user their roles back

~|users remove safety net
Removes the safety net for this server

~|users show safety net
Shows the safety net for this server, and explains some of the details
