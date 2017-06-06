import os
import logging
import tempfile
from lxml import etree
import subprocess
import pprint
import xmlrpc.client
from enum import Enum

import wishful_upis as upis
import wishful_framework as wishful_module


__author__ = "Maicon Kist"
__copyright__ = "Copyright (c) 2016, Connect Centre - Trinity College Dublin"
__version__ = "0.1.0"
__email__ = "kism@tcd.ie"

""" tracking the state of the radio program """
class RadioProgramState(Enum):
    INACTIVE = 1
    RUNNING = 2
    PAUSED = 3
    STOPPED = 4

class RadioProgramConf(object):
    def __init__(self, name, args, port, code, path):
        self.name = name
        self.args = args
        self.port = port
        self.code = code
        self.path = path

    def __eq__(self, other):
        if not isinstance(other, RadioProgramConf):
            return False
        if self.name == other.name:
            return True
        return False


    def __hash__(self):
        return self.name

"""
    Basic GNURadio connector module.

    Supported functionality:
    - activate_radio_program: pass name of RP and the flowgraph as XML GRC file
    - deactivate_radio_program: stop or pause RP
    - set_parameters/get_parameters: generic getter/setter functions to control GnuRadio RP at runtime
"""
@wishful_module.build_module
class GnuRadioModule(wishful_module.AgentModule):
    def __init__(self):
        super(GnuRadioModule, self).__init__()

        self.log = logging.getLogger('gnuradio_module.main')

        # list of all radio programs path
        self.gr_radio_programs = {}
        # dict from string to RadioProgramConf objs
        self.gr_radio_programs_conf = {}
        # name of program in execution/idle 
        self.gr_exec_name = None
        self.gr_state = RadioProgramState.INACTIVE

        self.gr_process = None
        self.gr_process_io = None

        self.gr_radio_programs_path = os.path.join(os.path.expanduser("~"), ".wishful", "radio")

        if not os.path.exists(self.gr_radio_programs_path):
            os.makedirs(self.gr_radio_programs_path)
            self._build_radio_program_dict()

        # config values
        self.ctrl_socket_host = "localhost"
        self.default_socket_port = 1235
        self.ctrl_socket = None

        self.combiner = None
        self.log.debug('initialized ...')

    def _exec_program(self, grc_radio_program_name, program_args):

        if self.gr_process_io is None:
                self.gr_process_io = {'stdout': open('/tmp/gnuradio.log', 'w+'), 'stderr': open('/tmp/gnuradio-err.log', 'w+')}

        if self.gr_process is not None:
                # An instance is already running
                self.gr_process.kill()
                self.gr_process = None

        try:
            the_program = self.gr_radio_programs_conf[grc_radio_program_name]

            # start GNURadio process
            self.log.info("Starting GNURADIO program " + the_program.name)
            self.gr_exec_name = grc_radio_program_name
            self.log.info(" ".join(["env","python2", the_program.path] + program_args))
            self.gr_process = subprocess.Popen(["env","python2",
                    the_program.path] + program_args, 
                    stdout=self.gr_process_io['stdout'], stderr=self.gr_process_io['stderr'])
            self.gr_state = RadioProgramState.RUNNING
            self.log.info("GNURadio process %s started succesfully" % (grc_radio_program_name))
        except:
                self.log.error("Failed to start GNURadio program %s" % (grc_radio_program_name))
                self.gr_process_io = None
                self.gr_process = None
                self.gr_exec_name = None
                return False
        return True

    def _remove_program(self, grc_radio_program_name):
        """ Remove radio program from local repository """
        if grc_radio_program_name not in self.gr_radio_programs_conf:
            self.log.info("Could not remove program '%s': not registered" % (grc_radio_program_name, ) )

        the_program = self.gr_radio_programs_conf[grc_radio_program_name]

        if os.path.isfile(the_program.path):
            os.remove(the_program.path)

        # if the program is GRC, we need to also remove the generated python file
        if the_program.path.endswith('grc'):
            pyfile = os.path.splitext(the_program.path)[0] + '.py'
            if os.path.isfile(pyfile):
                os.remove(pyfile)

        # remove from our configuration dict
        del self.gr_radio_programs_conf[grc_radio_program_name]

    def _close_gr_process(self):
        if self.gr_process is not None and hasattr(self.gr_process, "kill"):
            self.gr_process.kill()

        if self.gr_process_io is not None and self.gr_process_io is dict:
            for k in self.gr_process_io.keys():
                if not self.gr_process_io[k].closed:
                    self.gr_process_io[k].close()
                    self.gr_process_io[k] = None

        self.ctrl_socket = None

    def _convert_grc_to_python(self, grc_radio_program_name, grc_radio_program_code):
        # change the id value, so when we convert we generate a specific .py filename
        grc_radio_program_code = bytes(bytearray(grc_radio_program_code, encoding='utf-8'))

        #xmltext = etree.XML(grc_radio_program_code)
        xmltext = etree.fromstring(grc_radio_program_code)
        if xmltext.xpath('//block/param/key')[0].text == 'id':
           xmltext.xpath('//block/param/value')[0].text  = grc_radio_program_name

        tmpfile = tempfile.NamedTemporaryFile(delete=False)
        tmpfile.write(etree.tostring(xmltext))
        tmpfile.close()

        # GRCC will convert the grc file to the directory specified + the flowgraph name + .py
        py_filename = os.path.join(self.gr_radio_programs_path, grc_radio_program_name + '.py')

        self.log.info("grcc --directory=" + self.gr_radio_programs_path + " " + tmpfile.name)
        try:
           # the correct way would be to create this in a tmp folder
           self.log.info("grcc --directory=" + self.gr_radio_programs_path + " " + tmpfile.name)
           subprocess.check_call(["grcc", "--directory=" + self.gr_radio_programs_path, tmpfile.name])
        except:
           self.log.info('could not execute grcc compiler')

        with open(py_filename, "r") as py_fid:
           grc_radio_program_code = py_fid.read();
        os.remove(py_filename)

        return grc_radio_program_code


    @wishful_module.bind_function(upis.radio.get_running_radio_program)
    def get_running_radio_program(self):
        if self.gr_state == RadioProgramState.RUNNING:
            return self.gr_exec_name
        else:
            return None

    @wishful_module.bind_function(upis.radio.activate_radio_program)
    def set_active(self, args):

        program_name = args['program_name'] if 'program_name' in args else None
        program_code = args['program_code'] if 'program_code' in args else None
        program_args = args['program_args'] if 'program_args' in args else ['', ]
        program_type = args['program_type'] if 'program_type' in args else 'grc'
        program_port = args['program_port'] if 'program_port' in args else self.default_socket_port

        if self.gr_state == RadioProgramState.INACTIVE:
            self.log.info("Start new radio program")

            # convert from xml (.grc file) to a python file
            if program_type == "grc":
                program_code = self._convert_grc_to_python(program_name, program_code)
            elif program_type != 'py':
                self.log.error("program_type must be either 'grc' or 'py' -> received %s" % (program_type, ))
                return

            if program_name not in self.gr_radio_programs_conf:
               # serialize radio program to local repository
               path = self._add_program_to_repo(program_name, program_code)
               # create a conf obj with everything
               self.gr_radio_programs_conf[program_name] = RadioProgramConf(program_name, program_args, program_port, program_code, path)

            """Launches Gnuradio in background"""
            return self._exec_program(program_name, program_args)

        elif self.gr_state == RadioProgramState.PAUSED and self.gr_exec_name == program_name:
            # wakeup
            self.log.info('Wakeup radio program')
            self._init_proxy(self.gr_exec_name)

            # TODO: check if connection was succesfull
            self.gr_state = RadioProgramState.RUNNING

        else:
            self.log.warn('Please deactive old radio program before activating a new one.')


    @wishful_module.bind_function(upis.radio.deactivate_radio_program)
    def set_inactive(self, radio_program_name, pause = False):

        if radio_program_name != self.gr_exec_name:
            self.log.info('Program {} is not running. Running program is {}'.format(radio_program_name, self.gr_exec_name))
            return

        if self.gr_state == RadioProgramState.RUNNING or self.gr_state == RadioProgramState.PAUSED:
            if pause:
                self.log.info("Pausing radio program")
                self.ctrl_socket.stop()
                self.ctrl_socket.wait()
                self.gr_state = RadioProgramState.PAUSED
            else:
                self.log.info("Stopping radio program")
                self.gr_state = RadioProgramState.INACTIVE
                self._close_gr_process()
                self._remove_program(radio_program_name)
        else:
            self.log.warn("no running or paused radio program; ignore command")

    @wishful_module.bind_function(upis.radio.set_parameters)
    def gnuradio_set_vars(self, param_key_values_dict):
        if self.gr_state == RadioProgramState.RUNNING or self.gr_state == RadioProgramState.PAUSED:
            self._init_proxy(self.gr_exec_name)
            for k, v in param_key_values_dict.items():
                try:
                    getattr(self.ctrl_socket, "set_%s" % k)(v)
                except Exception as e:
                    self.log.error("Unknown variable '%s -> %s'" % (k, e))
        else:
            self.log.warn("no running or paused radio program; ignore command")

    @wishful_module.bind_function(upis.radio.get_parameters)
    def gnuradio_get_vars(self, param_key_list):
        if self.gr_state == RadioProgramState.RUNNING or self.gr_state == RadioProgramState.PAUSED:
            rv = {}
            self._init_proxy(self.gr_exec_name)
            for k in param_key_list:
                try:
                    self.log.info("Probing for variable '%s'" % (k,))
                    res = getattr(self.ctrl_socket, "get_%s" % k)
                    rv[k] = res()
                except Exception as e:
                    self.log.error("Unknown variable '%s -> %s'" % (k, e))

            self.log.info("Returning: " + str(rv))
            return rv
        else:
            self.log.warn("no running or paused radio program; ignore command")
            return None

    def _add_program_to_repo(self, grc_radio_program_name, grc_radio_program_code):
        """ Serialize radio program to local repository """
        self.log.info("Add radio program %s to local repository" % grc_radio_program_name)

        path = os.path.join(self.gr_radio_programs_path, grc_radio_program_name + ".py")

        # serialize radio program XML flowgraph to file
        fid = open(path, 'w')

        fid.write(grc_radio_program_code)
        fid.close()

        # rebuild radio program dictionary
        self._build_radio_program_dict()

        return path

    def _build_radio_program_dict(self):
        """
            Converts the radio program XML flowgraphs into executable python scripts
        """
        return
        self.gr_radio_programs = {}
        grc_files = dict.fromkeys([x.rstrip(".grc") for x in os.listdir(self.gr_radio_programs_path) if x.endswith(".grc")], 0)
        topblocks = dict.fromkeys(
            [x for x in os.listdir(self.gr_radio_programs_path) if os.path.isdir(os.path.join(self.gr_radio_programs_path, x))], 0)
        for x in grc_files.keys():
            grc_files[x] = os.stat(os.path.join(self.gr_radio_programs_path, x + ".grc")).st_mtime
            try:
                os.mkdir(os.path.join(self.gr_radio_programs_path, x))
                topblocks[x] = 0
            except OSError:
                pass
        for x in topblocks.keys():
            topblocks[x] = os.stat(os.path.join(self.gr_radio_programs_path, x, 'top_block.py')).st_mtime if os.path.isfile(
                os.path.join(self.gr_radio_programs_path, x, 'top_block.py')) else 0
        for x in grc_files.keys():
            if grc_files[x] > topblocks[x]:
                outdir = "--directory=%s" % os.path.join(self.gr_radio_programs_path, x)
                input_grc = os.path.join(self.gr_radio_programs_path, x + ".grc")
                try:
                    subprocess.check_call(["grcc", outdir, input_grc])
                except:
                    pass
        for x in topblocks.keys():
            if os.path.isfile(os.path.join(self.gr_radio_programs_path, x, 'top_block.py')):
                self.gr_radio_programs[x] = os.path.join(self.gr_radio_programs_path, x, 'top_block.py')

        self.log.info('gr_radio_programs:\n{}'.format(pprint.pformat(self.gr_radio_programs)))


    def _init_proxy(self, program_name):
        if self.ctrl_socket != None:
            self.log.info("Already connected to a GNURadio process")
            return

        try:
            port = self.gr_radio_programs_conf[program_name].port

            self.ctrl_socket = xmlrpc.client.ServerProxy("http://%s:%d" % (self.ctrl_socket_host, port))
            self.log.info("Connected to GNURadio process")

        except Exception as e:
             self.log.error("Error connecting to GNURadio process -> %s" % (e, ))
