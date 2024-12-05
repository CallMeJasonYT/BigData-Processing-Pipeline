from uxsim import *
import itertools
import pandas as pd

seed=0

W = World(
    name="",    # Scenario name
    deltan = 5,   # Simulation aggregation unit delta n
    tmax = 3600,  # Total simulation time (s)
    print_mode=1, save_mode=0, show_mode=1,    # Various options
    random_seed=seed,    # Set the random seed
    duo_update_time=600
)

W.addNode("orig1", 0, 0) #Create a node. Parameters: node name, visualization x-coordinate, visualization y-coordinate
W.addNode("orig2", 0, 2)
W.addNode("merge", 1, 1)
W.addNode("dest", 2, 1)

W.addLink("link1", "orig1", "merge", length=1000, free_flow_speed=20, jam_density=0.2, merge_priority=0.5) # Create a link. Parameters: link name, start node, end node, length, free_flow_speed, jam_density, merge_priority during merging
W.addLink("link2", "orig2", "merge", length=1000, free_flow_speed=20, jam_density=0.2, merge_priority=2)
W.addLink("link3", "merge", "dest", length=1000, free_flow_speed=20, jam_density=0.2)

W.adddemand("orig1", "dest", 0, 1800, 0.4) # Create OD traffic demand. Parameters: origin node, destination node, start time, end time, demand flow rate
W.adddemand("orig2", "dest", 1800, 3600, 0.6)

W.exec_simulation()

# Convert vehicles data to a pandas DataFrame
df = W.analyzer.vehicles_to_pandas()

df.to_csv("sim_data.csv")