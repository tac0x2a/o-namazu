

def read_all(file_paht: str, conf) -> str:
    with open(file_paht) as f:
        return f.read()


def tail(file_paht: str, already_read_pos: int, conf) -> (str, int):

    with open(file_paht) as f:
        read_string = f.readline()  # header
        print(f'header:{read_string}')
        current = max(f.tell(), already_read_pos)
        f.seek(current)

        read_string += f.read()

        return (read_string, f.tell())

