angular.module(PKG.name+'.feature.dashboard')
  .config(function ($stateProvider, $urlRouterProvider) {

    $urlRouterProvider
      .when('/dashboard', '/dashboard/0');


    /**
     * State Configurations
     */
    $stateProvider

      .state('dashboard', {
        data: {
          highlightTab: 'operations'
        },
        url: '/dashboard/:tab',
        templateUrl: '/assets/features/dashboard/main.html',
        controller: 'DashboardCtrl'
      })

        .state('dashboard.addwdgt', {
          // url: '/widget/add',
          onEnter: function ($state, $modal) {
            $modal({
              template: '/assets/features/dashboard/partials/addwdgt.html'
            }).$promise.then(function () {
              $state.go('^', $state.params);
            });
          }
        })

      ;


  });
