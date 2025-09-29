package route_test

import (
	_ "errors"
	"time"

	"code.cloudfoundry.org/gorouter/route"
	"code.cloudfoundry.org/gorouter/test_util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("HashBased", func() {
	var (
		pool   *route.EndpointPool
		logger *test_util.TestLogger
	)

	BeforeEach(func() {
		logger = test_util.NewTestLogger("test")
		pool = route.NewPool(&route.PoolOpts{
			Logger:                 logger.Logger,
			RetryAfterFailure:      2 * time.Minute,
			Host:                   "",
			ContextPath:            "",
			MaxConnsPerBackend:     0,
			LoadBalancingAlgorithm: "hash",
		})
	})

	Describe("Next", func() {

		Context("when pool is empty", func() {
			It("does not select an endpoint", func() {
				iter := route.NewHashBased(logger.Logger, pool, "", false, false, "")
				Expect(iter.Next(0)).To(BeNil())
			})
		})

		Context("when pool has endpoints", func() {
			var (
				endpoints []*route.Endpoint
			)
			BeforeEach(func() {
				e1 := route.NewEndpoint(&route.EndpointOpts{Host: "1.2.3.4", Port: 5678, LoadBalancingAlgorithm: "hash", HashHeaderName: "tenant-id", PrivateInstanceId: "ID1"})
				e2 := route.NewEndpoint(&route.EndpointOpts{Host: "2.2.3.4", Port: 5678, LoadBalancingAlgorithm: "hash", HashHeaderName: "tenant-id", PrivateInstanceId: "ID2"})
				endpoints = []*route.Endpoint{e1, e2}
				for _, e := range endpoints {
					pool.Put(e)
				}

			})
			It("It returns the same endpoint for the same header value", func() {
				iter := route.NewHashBased(logger.Logger, pool, "", false, false, "")
				iter.(*route.HashBased).HeaderValue = "tenant-1"
				first := iter.Next(0)
				second := iter.Next(0)
				Expect(first).NotTo(BeNil())
				Expect(second).NotTo(BeNil())
				Expect(first).To(Equal(second))
			})
		})
	})
})
