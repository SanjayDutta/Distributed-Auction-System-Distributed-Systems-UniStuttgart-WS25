class Ring:
    def __init__(self, my_uuid, peers_dict):
        """
        :param my_uuid: UUID of the current node
        :param peers_dict: Dictionary of {uuid: ip} of all discovered peers
        """
        self.my_uuid = my_uuid
        self.all_nodes = list(peers_dict.keys())
        
        # Add myself to the list if not present (logic safety)
        if self.my_uuid not in self.all_nodes:
            self.all_nodes.append(self.my_uuid)
            
        # SORTING is the key to Ring formation.
        # Everyone must sort exactly the same way to agree on the ring order.
        self.all_nodes.sort()
        
        self.ring_order = self.all_nodes
        self.left_neighbor = None
        self.right_neighbor = None
        
        self._calculate_neighbors()

    def _calculate_neighbors(self):
        try:
            my_index = self.ring_order.index(self.my_uuid)
            total_nodes = len(self.ring_order)
            
            # Left Neighbor (Previous in list, wrap to end if first)
            left_index = (my_index - 1) % total_nodes
            self.left_neighbor = self.ring_order[left_index]
            
            # Right Neighbor (Next in list, wrap to start if last)
            right_index = (my_index + 1) % total_nodes
            self.right_neighbor = self.ring_order[right_index]
            
            print(f"[Ring] Formed! I am {self.my_uuid[:8]}.")
            print(f"       Left: {self.left_neighbor[:8]} | Right: {self.right_neighbor[:8]}")
            
        except ValueError:
            print("[Ring Error] Could not find myself in the node list!")

    def get_neighbors(self):
        return self.left_neighbor, self.right_neighbor
