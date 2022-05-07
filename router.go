package broker

type Routes map[string]*route
type HTTPRoutes map[string]*httpRoute

type MessageHandler func(msg Message)

type route struct {
	key      string
	queue    string
	exchange string
	handler  MessageHandler
}

type httpRoute struct {
	source  string
	handler MessageHandler
}

type messageRouter struct {
	routes     Routes
	httpRoutes HTTPRoutes
}

func (router *messageRouter) addRoute(r *route) {
	router.routes[r.key] = r
}

func (router *messageRouter) addHTTPRoute(r *httpRoute) {
	router.httpRoutes[r.source] = r
}

func (router *messageRouter) routeMessage(msg Message) {
	route, ok := router.routes[msg.GetRouteKey()]
	if !ok {
		return
	}

	route.handler(msg)
}
