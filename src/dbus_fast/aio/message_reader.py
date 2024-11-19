import logging
import socket
from functools import partial
from typing import Callable, Optional
import json
from pprint import pprint

from .._private.unmarshaller import Unmarshaller
from ..message import Message


def _message_reader(
    unmarshaller: Unmarshaller,
    process: Callable[[Message], None],
    finalize: Callable[[Optional[Exception]], None],
    negotiate_unix_fd: bool,
) -> None:
    print( "in \\dbus-fast\\src\\dbus_fast\\aio\\message_reader, _message_reader, enter" )
    """Reads messages from the unmarshaller and passes them to the process function."""
    try:
        while True:
            message = unmarshaller._unmarshall()
            if message is None:
                return
            try:
                print( "in \\dbus-fast\\src\\dbus_fast\\aio\\message_reader, _message_reader, before print message" )
                print( f"==>> msg-reader-messag:" )
                print( f"   ==>> Message={message.message_type.name} " )
                print( f"   ==>> serial={message.serial} " )
                print( f"   ==>> reply_serial={message.reply_serial} " )
                print( f"   ==>> sender={message.sender} " )
                print( f"   ==>> destination={message.destination} " )
                print( f"   ==>> path={message.path} " )
                print( f"   ==>> interface={message.interface} " )
                print( f"   ==>> member={message.member} " )
                print( f"   ==>> error_name={message.error_name} " )
                print( f"   ==>> signature={message.signature} " )
                print( f"   ==>> body={message.body}" )
                
#                pprint( message.body )
#                temp = {}
#                try:
#                    temp = dict( eval( str( message.body ) ) )
#                except Exception as e:
#                    print( f"failed to convert into dictionary, e={e}" )
#                print( f"temp={temp}" )
                
                
#                temp = str( message.body )
#                temp = temp.replace( '<', '"<' ) 
#                temp = temp.replace( '>', '>"' ) 
#                print( f"==>> message.body-1={temp}" )
#                print( f"==>> message.body-2={json.dumps(temp, sort_keys=True, indent=4)}" )
#                temp3 = eval( temp )
#                temp2 = {}
#                try:
#                    temp2 = {item['name']:item for item in temp3}
#                except Exception as e:
#                    print( "failed to convert into dictionary" )
#                print( f"==>> message.body-3={json.dumps(temp2, sort_keys=True, indent=4)}" )
                print( "in \\dbus-fast\\src\\dbus_fast\\aio\\message_reader, _message_reader, after print message, before process(message), not sure where this goes" )
                process(message)
                print( "in \\dbus-fast\\src\\dbus_fast\\aio\\message_reader, _message_reader, after print message, after process(message)" )
            except Exception:
                logging.error("Unexpected error processing message: %s", exc_info=True)
            # If we are not negotiating unix fds, we can stop reading as soon as we have
            # the buffer is empty as asyncio will call us again when there is more data.
            if (
                not negotiate_unix_fd
                and not unmarshaller._has_another_message_in_buffer()
            ):
                return
    except Exception as e:
        finalize(e)
    print( "in \\dbus-fast\\src\\dbus_fast\\aio\\message_reader, _message_reader, exit" )


def build_message_reader(
    sock: Optional[socket.socket],
    process: Callable[[Message], None],
    finalize: Callable[[Optional[Exception]], None],
    negotiate_unix_fd: bool,
) -> Callable[[], None]:
    print( "in \\dbus-fast\\src\\dbus_fast\\aio\\message_reader, build_message_reader, enter" )
    """Build a callable that reads messages from the unmarshaller and passes them to the process function."""
    unmarshaller = Unmarshaller(None, sock, negotiate_unix_fd)
    return partial(_message_reader, unmarshaller, process, finalize, negotiate_unix_fd)
