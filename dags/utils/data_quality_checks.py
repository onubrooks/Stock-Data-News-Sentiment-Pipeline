def check_completeness(pl_df, column_name):
    return ['PASS' if pl_df[column_name].is_not_null().all() else 'FAIL']


def check_data_quality(validation_results):
    if "FAIL" not in validation_results:
        return True
    return False


def check_data_quality_instance(df, column_name, data):
    return (
        'clean_{}_data'.format(data)
        if check_data_quality(check_completeness(df, column_name))
        else 'stop_{}_pipeline'.format(data)
    )
