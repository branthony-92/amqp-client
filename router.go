package broker

type MessageHandler func(msg Message)

type route struct {
	key      string
	queue    string
	exchange string
	handler  MessageHandler
}

type messageRouter struct {
	routes map[string]*route
}

func (router *messageRouter) addRoute(r *route) {
	router.routes[r.key] = r
}

func (router *messageRouter) routeMessage(msg Message) {
	route, ok := router.routes[msg.GetRouteKey()]
	if !ok {
		return
	}

	route.handler(msg)
}
