from pprint import pprint


def validate_obj(name, obj):
    for k, v in obj.items():
        assert v is None or isinstance(v, (int, float, bool, str)), '{}: invalid value for key {}: {}'.format(name, k, v)


def multi_dict_generator(defaults, multi_values, objs):
    validate_obj('defaults', defaults)
    for i, obj in enumerate(objs, 1):
        validate_obj('objs #{}'.format(i), obj)
    for k, values in multi_values.items():
        assert isinstance(values, list), 'invalid multi values for key {}: {}'.format(k, values)
    if len(multi_values) > 0:
        for key, values in multi_values.items():
            new_objs = []
            for value in values:
                for obj in objs:
                    new_objs.append({**defaults, **obj, key: value})
            objs = new_objs
    else:
        new_objs = []
        for obj in objs:
            new_objs.append({**defaults, **obj})
        objs = new_objs
    return objs


if __name__ == '__main__':
    objs = multi_dict_generator(
        {
            "force_skip_add_clear_prepare": True,
            "objects": 100,
            "duration_seconds": 600,
            "obj_size_kb": 100,
            "num_base_servers": 4,
            "base_servers_all_eu": True,
            "only_test_method": None,
            "load_generator": "custom",
        },
        {
            "concurrency": [1, 5],
            "num_extra_eu_servers": [1, 4],
            "number_of_random_domain_names": [10, 25, 50]
        },
        [
            {"obj_size_kb": 100, "make_put_or_del_every_iterations": 1000},
            {"obj_size_kb": 1000, "make_put_or_del_every_iterations": 5000},
            {"obj_size_kb": 10000, "make_put_or_del_every_iterations": 20000}
        ]
    )
    pprint([(obj['obj_size_kb'], obj['make_put_or_del_every_iterations'], obj['concurrency'], obj['num_extra_eu_servers'], obj['number_of_random_domain_names']) for obj in objs])
    pprint(len(objs))
