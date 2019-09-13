# ImputationTypes class
A class containing imputation descriptions and codes found in IPEDS database tables.

## Class Attributes
Attribute names in this class correspond to codes in an IPEDS table. 
| Attribute | Code |
|-----------|-------------|
| `analyst_corrected` | `'C'` 
| `carry_forward_procedure` | `'P'` |
| `data_not_usable` | `'H'` |
| `do_not_know` | `'D'` |
| `generated_from_other_values` | `'G'` |
| `group_median_procedure` | `'L'` |
| `implied_zero` | `'Z'` |
| `left_blank` | `'B'` |
| `logical_imputation` | `'L'` |
| `nearest_neighbor_procedure` | `'N'` |
| `not_applicable` | `'A'` |
| `ratio_adjustment` | `'K'` |
| `reported` | `'R'` |

## Methods
### type_to_code(cls,type)
    Returns the code string for the given imputation type. The argument `type` is a string of the imputation type.
### code_to_type(cls,code)
    Returns the imputation type as a string for the given string code