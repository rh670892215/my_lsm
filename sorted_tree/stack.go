package sorted_tree

// Stack 栈
type Stack struct {
	stack []*TreeNode
	// 栈顶指针
	top int
	// 栈底指针
	bottom int
}

// InitStack 初始化栈
func InitStack(n int) Stack {
	return Stack{
		stack:  make([]*TreeNode, n),
		top:    0,
		bottom: 0,
	}
}

// Push 入栈
func (s *Stack) Push(node *TreeNode) {
	if s.top == len(s.stack) {
		s.stack = append(s.stack, node)
		s.top++
		return
	}
	s.stack[s.top] = node
	s.top++
}

// Pop 出栈
func (s *Stack) Pop() (*TreeNode, bool) {
	if s.top == s.bottom {
		return nil, false
	}
	s.top--
	return s.stack[s.top], true
}
