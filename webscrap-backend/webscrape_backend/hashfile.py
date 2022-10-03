class HashMap:

    def __init__(self,inp_dict):
        self.dict_value = inp_dict

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __hash__(self):
        flattened_dict = flatten_dict(self.dict_value)
        final_hash_data = []
        for key, value in flattened_dict.items():
            if isinstance(value, list):
                new_value = ",".join([str(tuple(each_val)) for each_val in value])
                final_hash_data.append((key, new_value))
                continue

            final_hash_data.append((key, value))
        
        return hash(tuple(final_hash_data))


def flatten_dict(dd, separator='_', prefix=''):
    return {prefix + separator + k if prefix else k : v
             for kk, vv in dd.items()
             for k, v in flatten_dict(vv, separator, kk).items()
             } if isinstance(dd, dict) else { prefix : dd }
