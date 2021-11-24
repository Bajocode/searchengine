""" Scratchpad """
from copy import deepcopy
import uuid

Time = int

uuid1 = str(uuid.uuid4())
uuid2 = str(uuid.uuid4())

print(uuid1, uuid2, uuid1 < uuid2, uuid1 > uuid2)


class Link:
    """ Encapsulates all information about a link discovered by crawler """
    __slots__ = 'link_id', 'url', 'retrieved_at'

    def __init__(self, link_id: uuid.UUID, url: str, retrieved_at: Time):
        self.link_id = link_id
        self.url = url
        self.retrieved_at = retrieved_at

    def __deepcopy__(self, memo):  # memo is a dict of id's to copies
        id_self = id(self)        # memoization avoids unnecesary recursion
        copy_self = memo.get(id_self, False)
        if copy_self:
            return copy_self
        copy_self = type(self)(
            deepcopy(self.link_id, memo),
            deepcopy(self.url, memo),
            deepcopy(self.retrieved_at, memo))
        memo[id_self] = copy_self
        return copy_self


link1 = Link(uuid.uuid4(), url='', retrieved_at=0)
link2 = deepcopy(link1)

# print(link1.link_id, link1)
# print(link2.link_id, link2)


class Node:
    def __init__(self, v=None):
        self.parent, self.left, self.right, self.val = None, None, None, v


n1, n2, n3, n4, n5, n6, n7, n8, n9 = Node(1), Node(2), Node(
    3), Node(4), Node(5), Node(6), Node(7), Node(8), Node(9)
n1.left, n1.right = n2, n3
n2.parent, n2.left, n2.right = n1, n4, n5
n3.parent, n3.left, n3.right = n1, n6, n7
n4.parent, n4.left, n4.right = n2, n8, n9
n5.parent, n6.parent, n7.parent, n8.parent, n9.parent = n2, n3, n3, n4, n4


def dfs(root):
    if not root:
        return
    yield root
    yield from dfs(root.left)
    yield from dfs(root.right)


for node in dfs(n1):
    print(node.val)


def factors(n):
    for k in range(1, n+1):
        if n % k == 0:
            yield k
