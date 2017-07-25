import os
import time
import logging
import random
import wishful_upis as upis
import wishful_framework as wishful_module
import xmlrpc.client

from .generator.rp_combiner import RadioProgramCombiner
from .module_gnuradio import GnuRadioModule

__author__ = "Anatolij Zubow"
__copyright__ = "Copyright (c) 2015, Technische Universität Berlin"
__version__ = "0.1.0"
__email__ = "{zubow}@tkn.tu-berlin.de"

"""
    Advanced GNURadio connector module which supports fast switching between different GNURadio programs.
    1/ Call merge_radio_programs() to combine set of RPs
    2/ Call switch_radio_program() for fast switching between RPs at runtime

    Todo: mapping to UPIs is missing
"""

@wishful_module.build_module
class MultiGnuRadioModule(GnuRadioModule):
    def __init__(self):
        super(MultiGnuRadioModule, self).__init__()

        self.log = logging.getLogger('MultiGnuRadioModule')

    #@wishful_module.bind_function(upis.radio.merge_programs)
    def merge_programs(self, **kwargs):
        '''
            Given a set of Gnuradio programs (described as GRC flowgraph) this program combines all
            those radio programs in a single meta radio program which allows very fast switching from
            one protocol to another.
        '''

        # params
        grc_radio_program_names = kwargs['grc_radio_program_names']  # list of radio program names

        self.combiner = RadioProgramCombiner(self.gr_radio_programs_path)

        # make sure all radio programms are already uploaded
        for rp in grc_radio_program_names:
            if rp not in self.gr_radio_programs:
                self.log.warn('Cannot merge missing radio program!!!')
                raise AttributeError("Unknown radio program %s" % rp)
            self.combiner.add_radio_program(rp + '_', rp + '.grc')

        # run generator
        rp_fname = self.combiner.generate()

        # rebuild radio program dictionary
        self.build_radio_program_dict()

        return rp_fname

    #@wishful_module.bind_function(upis.radio.switch_program)
    def switch_program(self, target_program_name, **kwargs):
        '''
            Run-time control of meta radio program which allows very fast switching from
            one protocol to another:
            - context switching
        '''

        # open proxy
        proxy = xmlrpc.client.ServerProxy("http://localhost:8080/")

        # load metadata
        proto_usrp_src_dicts = eval(
            open(os.path.join(self.gr_radio_programs_path, 'meta_rp_proto_dict.txt'), 'r').read())
        usrp_source_fields = eval(open(os.path.join(self.gr_radio_programs_path, 'meta_rp_fields.txt'), 'r').read())

        res = getattr(proxy, "get_session_var")()
        self.log.info('Current proto: %s' % str(res))
        # last_proto = res[0]

        # get IDX of new radio program
        new_proto_idx = self.combiner.get_proto_idx(target_program_name)

        # read variables of new protocol
        init_session_value = []
        init_session_value.append(new_proto_idx)
        for field in usrp_source_fields:
            res = getattr(proxy, "get_%s" % proto_usrp_src_dicts[new_proto_idx][field])()
            init_session_value.append(float(res))

        self.log.info('Switch to protocol %d with cfg %s' % (new_proto_idx, str(init_session_value)))
        getattr(proxy, "set_session_var")(init_session_value)

