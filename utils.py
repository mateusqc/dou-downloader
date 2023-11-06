def revert_date_srt(date_str, separator = "-"):
    splitted = date_str.split(separator)
    return splitted[-1] + separator + splitted[1] + separator + splitted[0]