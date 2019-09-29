class ImputationTypes(object):
    '''Imputation types for an IpedsTable class
    '''

    analyst_corrected = 'C'
    carry_forward_procedure = 'P'
    data_not_usable = 'H'
    do_not_know = 'D'
    generated_from_other_values = 'G'
    group_median_procedure = 'L'
    implied_zero = 'Z'
    left_blank = 'B'
    logical_imputation = 'L'
    nearest_neighbor_procedure = 'N'
    not_applicable = 'A'
    ratio_adjustment = 'K'
    reported = 'R'

    @classmethod
    def type_to_code(cls, type):
        return cls.__dict__.get(type, None)

    @classmethod
    def code_to_type(cls, code):

        for k, v in ImputationTypes.__dict__.items():
            if k.startswith('_'): 
                continue
            if v == code.upper():
                return k
        return None
