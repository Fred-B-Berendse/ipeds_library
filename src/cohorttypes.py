class CohortTypes(object):
    '''Cohort types for various IpedsTables
    '''

    c_awlevelc_desc = { 1 : "Award < 1 academic year",
                        2 : "Award 1-4 academic years",
                        3 : "Associate's degree",
                        5 : "Bachelor's degree",
                        7 : "Master's degree", 
                        9 : "Doctor's degree", 
                        10 : "Postbaccalaureate or Post-master's certificate"
                      }

    c_awlevelc_str = { 1 : "awlt1yr",
                       2 : "awle4yrs",
                       3 : "awassoc",
                       5 : "awbach",
                       7 : "awmast", 
                       9 : "awdoct", 
                       10 : "awpbm"
                      }

    gr_cohort_desc = { 1 : "Bachelor's equiv + other 2011 subcohorts (4-yr institution)",
                       2 : "Bachelor's or equiv 2011 subcohort (4-yr institution)", 
                       3 : "Other degree/certif-seeking 2011 subcohort (4-yr institution)", 
                       4 : "Degree/certif-seeking students 2014 cohort ( 2-yr institution)"
                     }

    gr_cohort_str = { 1 : "cobaoth",
                      2 : "cobach", 
                      3 : "cooth4yr", 
                      4 : "cooth2yr"
                     }

    gr_chrtstat_desc = { 10 : "Revised cohort",
                         11 : "Exclusions", 
                         12 : "Revised cohort minus exclusions",
                         13 : "Completers: 150% time",
                         14 : "Completers: programs <2 years (<=150% time)",
                         15 : "Completers: programs 2-4 years (<=150% time)",
                         16 : "Completers: bachelor's or equiv (<=150% time)",
                         17 : "Completers: bachelor's or equiv (<=4 yrs)",
                         18 : "Completers: bachelor's or equiv (5 yrs)",
                         19 : "Completers: bachelor's or equiv (6 yrs)",
                         20 : "Transfer-out students",
                         22 : "Completers: <=100% time total",
                         23 : "Completers: programs <2 yrs (100% time) (no racial/gender data)",
                         24 : "Completers: programs 2-4 yrs (100% time) (no racial/gender data)",
                         31 : "Noncompleters, still enrolled",
                         32 : "Noncompleters, no longer enrolled"
                        }

    gr_chrtstat_str = { 10 : "cstrev",
                        11 : "cstexcl", 
                        12 : "cstrevex",
                        13 : "cstc150",
                        14 : "cstc1502yr",
                        15 : "cstc1504yr",
                        16 : "cstcball",
                        17 : "cstcb4yr",
                        18 : "cstcb5yr",
                        19 : "cstcb6yr",
                        20 : "csttrout",
                        22 : "cstc100",
                        23 : "cstc1002yr",
                        24 : "cstc1004yr",
                        31 : "cstenrl",
                        32 : "cstnenrl"
                      }

    grp_psgrtype_desc = { 1 : "Total 2011 cohort (4-year institution)",
                          2 : "Bachelor's degree 2011 cohort (4-year institution)",
                          3 : "Other degree/certificate 2011 cohort (4-year institution)",
                          4 : "Degree/certificate seeking 2014 cohort (<4-year institution)"
                        }

    grp_psgrtype_str = { 1 : "psgrall",
                         2 : "psgrbac",
                         3 : "psgro4yr",
                         4 : "psgro2yr"
                        }