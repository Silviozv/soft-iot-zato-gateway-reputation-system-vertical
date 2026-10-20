from fogledgerIota.iota.IotaBasic import (IotaBasic)
from fogledgerIota.iota.config.NodeConfig import (NodeConfig)
from fogledgerIota.iota.config.CoordConfig import (CoordConfig)
from fogledgerIota.iota.config.SpammerConfig import (SpammerConfig)
from fogledgerIota.iota.config.ApiConfig import (ApiConfig)
from fogledgerIota.iota.config.WebAppConfig import (WebAppConfig)
from typing import List
from fogbed import (
    VirtualInstance, setLogLevel, FogbedDistributedExperiment, Worker, Container, Controller, FogbedExperiment
)
import signal

setLogLevel('info')

if (__name__ == '__main__'):

    exp = FogbedExperiment()
    

    node0 = NodeConfig(name='node0', port_bindings={'8081':'1881', '1883':'1894', '14265':'14265'})
    

    cord = CoordConfig(name='cord', port_bindings={'8081':'8093'}, interval='60s')
    spammer = SpammerConfig(name='spammer', port_bindings={'8081':'8094'}, message ='one-click-tangle.')

    api = ApiConfig(name='api', port_bindings={'4000':'4000'})
    web_app = WebAppConfig(name='web_app', port_bindings={'8081':'80'})


    iota = IotaBasic(exp=exp, prefix='iota1', conf_nodes=[node0])

    print("Quantidade de instâncias virtuais: ", len(iota.ledgers))
    
    exp.add_link(iota.ledgers[0], iota.ledgers[1])
    exp.add_link(iota.ledgers[1], iota.ledgers[2])


    try:
        exp.start()
        iota.start_network()
        print("Experiment started")
        while True:
            key = input("\nPress 'p' to stop the experiment: ")
            if key.lower() == 'p':
                print("Stopping the experiment...")
                exp.stop()
                break
            else:
                print(f"Ignoring key: {key}")
    except Exception as ex:
        print(ex)
    finally:
        exp.stop()