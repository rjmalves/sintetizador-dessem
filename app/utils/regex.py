import re


def match_variables_with_wildcards(
    given_variables: list[str], all_variables: list[str]
) -> list[str]:
    variables_with_wildcards: list[str] = []
    for v in given_variables:
        if "*" in v:
            variables_with_wildcards += [
                matched_v
                for matched_v in all_variables
                if re.search(v.replace("*", ".*"), matched_v)
            ]
        else:
            variables_with_wildcards.append(v)
    return variables_with_wildcards
