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
				iter := route.NewHashBased(logger.Logger, pool, "", false, false, "meow-az")
				Expect(iter.Next(0)).To(BeNil())
			})
		})

		Context("when pool has endpoints", func() {
			var (
				endpoints []*route.Endpoint
			)
			BeforeEach(func() {
				e1 := route.NewEndpoint(&route.EndpointOpts{Host: "1.2.3.4", Port: 5678, LoadBalancingAlgorithm: "hash", HashHeaderName: "tenant-id", PrivateInstanceId: "ID1"})
				endpoints = []*route.Endpoint{e1}
				for _, e := range endpoints {
					pool.Put(e)
				}
			})
			It("It selects it", func() {
				iter := route.NewHashBased(logger.Logger, pool, "ID1", false, false, "meow-az")
				Expect(iter.Next(0)).NotTo(BeNil())
				Expect(iter.Next(0)).To(Equal(endpoints[0]))
			})
		})
	})
})
