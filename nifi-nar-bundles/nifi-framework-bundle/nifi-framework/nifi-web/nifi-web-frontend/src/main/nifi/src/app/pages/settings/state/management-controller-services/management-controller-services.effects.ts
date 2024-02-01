/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Injectable } from '@angular/core';
import { Actions, concatLatestFrom, createEffect, ofType } from '@ngrx/effects';
import * as ManagementControllerServicesActions from './management-controller-services.actions';
import * as ErrorActions from '../../../../state/error/error.actions';
import { catchError, from, map, of, switchMap, take, takeUntil, tap } from 'rxjs';
import { MatDialog } from '@angular/material/dialog';
import { ManagementControllerServiceService } from '../../service/management-controller-service.service';
import { Store } from '@ngrx/store';
import { NiFiState } from '../../../../state';
import { selectControllerServiceTypes } from '../../../../state/extension-types/extension-types.selectors';
import { CreateControllerService } from '../../../../ui/common/controller-service/create-controller-service/create-controller-service.component';
import { Client } from '../../../../service/client.service';
import { YesNoDialog } from '../../../../ui/common/yes-no-dialog/yes-no-dialog.component';
import { EditControllerService } from '../../../../ui/common/controller-service/edit-controller-service/edit-controller-service.component';
import {
    ComponentType,
    ControllerServiceReferencingComponent,
    UpdateControllerServiceRequest
} from '../../../../state/shared';
import { Router } from '@angular/router';
import { selectSaving, selectStatus } from './management-controller-services.selectors';
import { EnableControllerService } from '../../../../ui/common/controller-service/enable-controller-service/enable-controller-service.component';
import { DisableControllerService } from '../../../../ui/common/controller-service/disable-controller-service/disable-controller-service.component';
import { PropertyTableHelperService } from '../../../../service/property-table-helper.service';
import { HttpErrorResponse } from '@angular/common/http';
import { ErrorHelper } from '../../../../service/error-helper.service';

@Injectable()
export class ManagementControllerServicesEffects {
    constructor(
        private actions$: Actions,
        private store: Store<NiFiState>,
        private client: Client,
        private managementControllerServiceService: ManagementControllerServiceService,
        private errorHelper: ErrorHelper,
        private dialog: MatDialog,
        private router: Router,
        private propertyTableHelperService: PropertyTableHelperService
    ) {}

    loadManagementControllerServices$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ManagementControllerServicesActions.loadManagementControllerServices),
            concatLatestFrom(() => this.store.select(selectStatus)),
            switchMap(([, status]) =>
                from(this.managementControllerServiceService.getControllerServices()).pipe(
                    map((response) =>
                        ManagementControllerServicesActions.loadManagementControllerServicesSuccess({
                            response: {
                                controllerServices: response.controllerServices,
                                loadedTimestamp: response.currentTime
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        if (status === 'success') {
                            if (this.errorHelper.showErrorInContext(errorResponse.status)) {
                                return of(ErrorActions.snackBarError({ error: errorResponse.error }));
                            } else {
                                return of(this.errorHelper.fullScreenError(errorResponse));
                            }
                        } else {
                            return of(this.errorHelper.fullScreenError(errorResponse));
                        }
                    })
                )
            )
        )
    );

    openNewControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.openNewControllerServiceDialog),
                concatLatestFrom(() => this.store.select(selectControllerServiceTypes)),
                tap(([, controllerServiceTypes]) => {
                    const dialogReference = this.dialog.open(CreateControllerService, {
                        data: {
                            controllerServiceTypes
                        },
                        panelClass: 'medium-dialog'
                    });

                    dialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    dialogReference.componentInstance.createControllerService
                        .pipe(take(1))
                        .subscribe((controllerServiceType) => {
                            this.store.dispatch(
                                ManagementControllerServicesActions.createControllerService({
                                    request: {
                                        revision: {
                                            clientId: this.client.getClientId(),
                                            version: 0
                                        },
                                        controllerServiceType: controllerServiceType.type,
                                        controllerServiceBundle: controllerServiceType.bundle
                                    }
                                })
                            );
                        });
                })
            ),
        { dispatch: false }
    );

    createControllerService$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ManagementControllerServicesActions.createControllerService),
            map((action) => action.request),
            switchMap((request) =>
                from(this.managementControllerServiceService.createControllerService(request)).pipe(
                    map((response) =>
                        ManagementControllerServicesActions.createControllerServiceSuccess({
                            response: {
                                controllerService: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        this.dialog.closeAll();
                        return of(ErrorActions.snackBarError({ error: errorResponse.error }));
                    })
                )
            )
        )
    );

    createControllerServiceSuccess$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ManagementControllerServicesActions.createControllerServiceSuccess),
            map((action) => action.response),
            tap(() => {
                this.dialog.closeAll();
            }),
            switchMap((response) =>
                of(
                    ManagementControllerServicesActions.selectControllerService({
                        request: {
                            id: response.controllerService.id
                        }
                    })
                )
            )
        )
    );

    navigateToEditService$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.navigateToEditService),
                map((action) => action.id),
                tap((id) => {
                    this.router.navigate(['/settings', 'management-controller-services', id, 'edit']);
                })
            ),
        { dispatch: false }
    );

    openConfigureControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.openConfigureControllerServiceDialog),
                map((action) => action.request),
                tap((request) => {
                    const serviceId: string = request.id;

                    const editDialogReference = this.dialog.open(EditControllerService, {
                        data: {
                            controllerService: request.controllerService
                        },
                        id: serviceId,
                        panelClass: 'large-dialog'
                    });

                    editDialogReference.componentInstance.saving$ = this.store.select(selectSaving);

                    editDialogReference.componentInstance.createNewProperty =
                        this.propertyTableHelperService.createNewProperty(
                            request.id,
                            this.managementControllerServiceService
                        );

                    const goTo = (commands: string[], destination: string): void => {
                        if (editDialogReference.componentInstance.editControllerServiceForm.dirty) {
                            const saveChangesDialogReference = this.dialog.open(YesNoDialog, {
                                data: {
                                    title: 'Controller Service Configuration',
                                    message: `Save changes before going to this ${destination}?`
                                },
                                panelClass: 'small-dialog'
                            });

                            saveChangesDialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                                editDialogReference.componentInstance.submitForm(commands);
                            });

                            saveChangesDialogReference.componentInstance.no.pipe(take(1)).subscribe(() => {
                                editDialogReference.close('ROUTED');
                                this.router.navigate(commands);
                            });
                        } else {
                            editDialogReference.close('ROUTED');
                            this.router.navigate(commands);
                        }
                    };

                    editDialogReference.componentInstance.goToService = (serviceId: string) => {
                        const commands: string[] = ['/settings', 'management-controller-services', serviceId];
                        goTo(commands, 'Controller Service');
                    };

                    editDialogReference.componentInstance.goToReferencingComponent = (
                        component: ControllerServiceReferencingComponent
                    ) => {
                        const route: string[] = this.getRouteForReference(component);
                        goTo(route, component.referenceType);
                    };

                    editDialogReference.componentInstance.createNewService =
                        this.propertyTableHelperService.createNewService(
                            request.id,
                            this.managementControllerServiceService,
                            this.managementControllerServiceService,
                            null,
                            (createResponse) =>
                                this.store.dispatch(
                                    ManagementControllerServicesActions.inlineCreateControllerServiceSuccess({
                                        response: {
                                            controllerService: createResponse
                                        }
                                    })
                                )
                        );

                    editDialogReference.componentInstance.editControllerService
                        .pipe(takeUntil(editDialogReference.afterClosed()))
                        .subscribe((updateControllerServiceRequest: UpdateControllerServiceRequest) => {
                            this.store.dispatch(
                                ManagementControllerServicesActions.configureControllerService({
                                    request: {
                                        id: request.controllerService.id,
                                        uri: request.controllerService.uri,
                                        payload: updateControllerServiceRequest.payload,
                                        postUpdateNavigation: updateControllerServiceRequest.postUpdateNavigation
                                    }
                                })
                            );
                        });

                    editDialogReference.afterClosed().subscribe((response) => {
                        this.store.dispatch(ErrorActions.clearBannerErrors());

                        if (response != 'ROUTED') {
                            this.store.dispatch(
                                ManagementControllerServicesActions.selectControllerService({
                                    request: {
                                        id: serviceId
                                    }
                                })
                            );
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    configureControllerService$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ManagementControllerServicesActions.configureControllerService),
            map((action) => action.request),
            switchMap((request) =>
                from(this.managementControllerServiceService.updateControllerService(request)).pipe(
                    map((response) =>
                        ManagementControllerServicesActions.configureControllerServiceSuccess({
                            response: {
                                id: request.id,
                                controllerService: response,
                                postUpdateNavigation: request.postUpdateNavigation
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) => {
                        if (this.errorHelper.showErrorInContext(errorResponse.status)) {
                            return of(
                                ManagementControllerServicesActions.managementControllerServicesBannerApiError({
                                    error: errorResponse.error
                                })
                            );
                        } else {
                            this.dialog.getDialogById(request.id)?.close('ROUTED');
                            return of(this.errorHelper.fullScreenError(errorResponse));
                        }
                    })
                )
            )
        )
    );

    managementControllerServicesBannerApiError$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ManagementControllerServicesActions.managementControllerServicesBannerApiError),
            map((action) => action.error),
            switchMap((error) => of(ErrorActions.addBannerError({ error })))
        )
    );

    configureControllerServiceSuccess$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.configureControllerServiceSuccess),
                map((action) => action.response),
                tap((response) => {
                    if (response.postUpdateNavigation) {
                        this.router.navigate(response.postUpdateNavigation);
                        this.dialog.getDialogById(response.id)?.close('ROUTED');
                    } else {
                        this.dialog.closeAll();
                    }
                })
            ),
        { dispatch: false }
    );

    openEnableControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.openEnableControllerServiceDialog),
                map((action) => action.request),
                tap((request) => {
                    const serviceId: string = request.id;

                    const enableDialogReference = this.dialog.open(EnableControllerService, {
                        data: request,
                        id: serviceId,
                        panelClass: 'large-dialog'
                    });

                    enableDialogReference.componentInstance.goToReferencingComponent = (
                        component: ControllerServiceReferencingComponent
                    ) => {
                        const route: string[] = this.getRouteForReference(component);
                        this.router.navigate(route);
                    };

                    enableDialogReference.afterClosed().subscribe((response) => {
                        if (response != 'ROUTED') {
                            this.store.dispatch(ManagementControllerServicesActions.loadManagementControllerServices());
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    openDisableControllerServiceDialog$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.openDisableControllerServiceDialog),
                map((action) => action.request),
                tap((request) => {
                    const serviceId: string = request.id;

                    const enableDialogReference = this.dialog.open(DisableControllerService, {
                        data: request,
                        id: serviceId,
                        panelClass: 'large-dialog'
                    });

                    enableDialogReference.componentInstance.goToReferencingComponent = (
                        component: ControllerServiceReferencingComponent
                    ) => {
                        const route: string[] = this.getRouteForReference(component);
                        this.router.navigate(route);
                    };

                    enableDialogReference.afterClosed().subscribe((response) => {
                        if (response != 'ROUTED') {
                            this.store.dispatch(ManagementControllerServicesActions.loadManagementControllerServices());
                        }
                    });
                })
            ),
        { dispatch: false }
    );

    promptControllerServiceDeletion$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.promptControllerServiceDeletion),
                map((action) => action.request),
                tap((request) => {
                    const dialogReference = this.dialog.open(YesNoDialog, {
                        data: {
                            title: 'Delete Controller Service',
                            message: `Delete controller service ${request.controllerService.component.name}?`
                        },
                        panelClass: 'small-dialog'
                    });

                    dialogReference.componentInstance.yes.pipe(take(1)).subscribe(() => {
                        this.store.dispatch(
                            ManagementControllerServicesActions.deleteControllerService({
                                request
                            })
                        );
                    });
                })
            ),
        { dispatch: false }
    );

    deleteControllerService$ = createEffect(() =>
        this.actions$.pipe(
            ofType(ManagementControllerServicesActions.deleteControllerService),
            map((action) => action.request),
            switchMap((request) =>
                from(this.managementControllerServiceService.deleteControllerService(request)).pipe(
                    map((response) =>
                        ManagementControllerServicesActions.deleteControllerServiceSuccess({
                            response: {
                                controllerService: response
                            }
                        })
                    ),
                    catchError((errorResponse: HttpErrorResponse) =>
                        of(ErrorActions.snackBarError({ error: errorResponse.error }))
                    )
                )
            )
        )
    );

    selectControllerService$ = createEffect(
        () =>
            this.actions$.pipe(
                ofType(ManagementControllerServicesActions.selectControllerService),
                map((action) => action.request),
                tap((request) => {
                    this.router.navigate(['/settings', 'management-controller-services', request.id]);
                })
            ),
        { dispatch: false }
    );

    private getRouteForReference(reference: ControllerServiceReferencingComponent): string[] {
        if (reference.referenceType == 'ControllerService') {
            if (reference.groupId == null) {
                return ['/settings', 'management-controller-services', reference.id];
            } else {
                return ['/process-groups', reference.groupId, 'controller-services', reference.id];
            }
        } else if (reference.referenceType == 'ReportingTask') {
            return ['/settings', 'reporting-tasks', reference.id];
        } else if (reference.referenceType == 'Processor') {
            return ['/process-groups', reference.groupId, ComponentType.Processor, reference.id];
        } else if (reference.referenceType == 'FlowAnalysisRule') {
            return ['/settings', 'flow-analysis-rules', reference.id];
        } else if (reference.referenceType == 'ParameterProvider') {
            return ['/settings', 'parameter-providers', reference.id];
        } else {
            return ['/settings', 'registry-clients', reference.id];
        }
    }
}