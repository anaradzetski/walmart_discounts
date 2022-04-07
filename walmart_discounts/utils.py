def json_dive(json_, dive_sequence):
    ans = json_
    for dive_item in dive_sequence:
        ans = ans[dive_item]
    return ans
