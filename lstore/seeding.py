class Node:
    def __init__(self, value):
        self.value = value
        self.left = None
        self.right = None


class SeedSet:
    def __init__(self, arr):
        self.root = self._build_tree(sorted(arr))

    def _build_tree(self, arr):
        if not arr:
            return None
        mid = len(arr) // 2
        root = Node(arr[mid])
        root.left = self._build_tree(arr[:mid])
        root.right = self._build_tree(arr[mid + 1 :])
        return root

    def search(self, x):
        node = self.root
        while node:
            if node.value == x:
                return node
            elif node.value < x:
                node = node.right
            else:
                node = node.left
        return None

    def add(self, x):
        if not self.root:
            self.root = Node(x)
        else:
            parent = None
            node = self.root
            while node:
                if node.value == x:
                    return
                elif node.value < x:
                    parent = node
                    node = node.right
                else:
                    parent = node
                    node = node.left
            if parent.value < x:
                parent.right = Node(x)
            else:
                parent.left = Node(x)

    def remove(self, x):
        node: Node = self.search(x)
        if not node:
            return
        if node == self.root:
            if not node.left and not node.right:
                self.root = None
            elif node.left and not node.right:
                self.root = node.left
            elif node.right and not node.left:
                self.root = node.right
            else:
                min_node: Node = node.right
                while min_node.left:
                    min_node = min_node.left
                self.remove(min_node.value)
                min_node.left = node.left
                min_node.right = node.right
                self.root = min_node
        else:
            parent = self._find_parent(node)
            if not node.left and not node.right:
                if parent.left == node:
                    parent.left = None
                else:
                    parent.right = None
            elif node.left and not node.right:
                if parent.left == node:
                    parent.left = node.left
                else:
                    parent.right = node.left
            elif node.right and not node.left:
                if parent.left == node:
                    parent.left = node.right
                else:
                    parent.right = node.right
            else:
                min_node = node.right
                while min_node.left:
                    min_node = min_node.left
                self.remove(min_node.value)
                min_node.left = node.left
                min_node.right = node.right
                if parent.left == node:
                    parent.left = min_node
                else:
                    parent.right = min_node

    def _find_parent(self, node):
        parent = None
        current = self.root
        while current:
            if current == node:
                return parent
            elif node.value < current.value:
                parent = current
                current = current.left
            else:
                parent = current
                current = current.right
        return None

    def search_range(self, low, high):
        result = []
        self._search_range_helper(self.root, low, high, result)
        return result

    def _search_range_helper(self, node: Node, low, high, result):
        if not node:
            return
        if node.value > low:
            self._search_range_helper(node.left, low, high, result)
        if low <= node.value <= high:
            result.append(node.value)
        if node.value < high:
            self._search_range_helper(node.right, low, high, result)
